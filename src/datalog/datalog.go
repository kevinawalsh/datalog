// Copyright (c) 2014, Kevin Walsh.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A Datalog engine.
package datalog

import (
	"bytes"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"fmt"
)

// Notes on uniqueness: The datalog engine must be able to tell when two
// variables are the "same". When variables are represented by distinct textual
// names, like "X" or "Y", this is trivial: just compare the text. This applies
// to constants, identifiers, and predicate symbols as well.
//
// As an optimization, the Lua implementation interns all variables (and
// identifiers, etc.) before processing. This step requires that: (1) each
// variable can be used as the key to a map; and (2) a variable can be stored as
// a value in a map without preventing garbage collection. The Lua
// implementation solves (1) using the textual names, and solves (2) using maps
// with weak references.
//
// All of the above is problematic in go. First, distinct textual names are only
// readily available when processing datalog written in text. When datalog is
// driven programmatically, assigning distinct textual names is a bother.
// Second, many values in go can't be used as keys in a map. In particular,
// literals can't be, since these are structs that containe slices. Finally, go
// doesn't provide weak references, so the typical approach to interning using a
// map would lead to garbage collection issues.
//
// This implementation uses a different approach. It allows a variety of pointer
// types to be used as variables, identifiers, constants, or predicate symbols.
// Two variables (etc.) are then considered the "same" if the pointers are
// equal, i.e. if they point to the same go object. It is the caller's
// responsibility to ensure that the same go object is used when the same
// variable is intended.
//
// There is a wrinkle, however: there is no way in go to express a constraint
// that only pointer types can be used as variables. We work around this by
// requiring variables to embed an anonymous Var struct. Only a pointer to [a
// struct containing] Var can be used as a variable.

// id is used to distinguish different variables, constants, etc.
type id uintptr

// Const represents a concrete datalog value that can be used as a term. Typical
// examples include alice, bob, "Hello", 42, -3, and other printable sequences.
// This implementation doesn't place restrictions on the contents.
type Const interface {
	// cID returns a distinct number for each live Const.
  cID() id
	Term
}

// DistinctConst can be embedded as an anonymous field in a struct T, enabling
// *T to be used as a Const. 
type DistinctConst struct {
	_ byte  // avoid confounding pointers due to zero size
}

func (p * DistinctConst) cID() id {
	return reflect.ValueOf(p).Pointer()
}

// Var represents a datalog variable. These are typically written with initial
// uppercase, e.g. X, Y, Left_child. This implementation doesn't restrict or
// even require variable names.
type Var interface {
	// vID returns a distinct number for each live Var.
  vID() id
	Term
}

// DistinctVar can be embedded as an anonymous field in a struct T, enabling *T
// to be used as a Var. In addition, &DistinctVar{} can be used as a fresh Var
// that has no name or associated data but is distinct from all other live Vars.
type DistinctVar struct {
	_ byte  // avoid confounding pointers due to zero size
}

func (p * DistinctVar) vID() id {
	return reflect.ValueOf(p).Pointer()
}

// Term represents an argument of a literal. Var and Const implement Term.
type Term interface {
	unify(other Term, env env) env
	unifyVar(other Var, env env) env
	unifyConst(other Const, env env) env
	chase(env env) Term
}

// Literal represents a predicate with terms for arguments. Typical examples
// include person(alice), ancestor(alice, bob), and ancestor(eve, X).
type Literal struct {
	Pred Pred
	Arg []Term
	cachedTag  *string
	cachedID   *string // TODO(kwalsh) remove
}

// NewLiteral constructs a new literal from a predicate and arguments. The
// number of arguments must match the predicate's arity, else nil is returned.
func NewLiteral(p Pred, arg ...Term) *Literal, error {
	if p.arity() != len(arg) {
		return nil, errors.New("datalog: arity mismatch")
	}
	return &Literal{Pred: p, Arg: arg}, nil
}

// tag returns a "variant tag" for a literal, such that two literals have the
// same variant tag if and only if they are identical modulo variable renaming.
func (l *Literal) tag() string {
	if l.cachedTag != nil {
		return *l.cachedTag
	}
	var buf bytes.Buffer
	l.tagf(&buf, make(map[id]int))
	tag := buf.String()
	l.cachedTag = &tag
	return tag
}

// tagf writes a literal's "variant tag" into buf after renaming variables
// according to the varNum map. If the varNum map is nil, then variables are not
// renamed.
func (l *Literal) tagf(buf *bytes.Buffer, varNum map[id]int) {
	// Tag encoding: hex(pred-id),term,term,...
	// with varMap, term consts are hex, term vars are "v0", "v1", ...
	// with no varMap, terms are all hex
	fmt.Fprintf(buf, "%x", l.Pred.pID())
	for _, arg := range l.Arg {
		switch arg := arg.(type) {
		case Const:
			fmt.Fprintf(buf, ",%x", arg.cID())
		case Var:
			if varNum == nil {
				fmt.Fprintf(buf, ",%x", arg.vID())
				panic("datalog: doesn't happen?")
			} else {
				vid := arg.vID()
				num, ok := varNum[vid]
				if !ok {
					num = len(varNum)
					varNum[vid] = num
				}
				fmt.Fprintf(buf, ",v%d", num)
			}
		default:
			panic("not reached")
		}
	}
}

// lID returns an "identity tag" for a literal, such that two literals have the
// same identity tag if and only if they are identical, including variables.
// TODO(kwalsh) Used by subgoal.facts[] map.
// TODO(kwalsh) eliminate this entirely:
func (l *Literal) lID() string {
	if l.cachedID != nil {
		return *l.cachedID
	}
	var buf bytes.Buffer
	l.tagf(&buf, nil)
	id := buf.String()
	l.cachedID = &id
	return id
}

// Clause has a head literal and zero or more body literals. With an empty
// body, it is known as a fact. Otherwise, a rule.
// Example fact: parent(alice, bob)
// Example rule: ancestor(A, C) :- ancestor(A, B), ancestor(B, C)
type Clause struct {
	Head *Literal
	Body []*Literal
}

// Pred represents a logical predicate, or relation, of a given arity.
type Pred interface {
	// pID returns a distinct number for each live Pred.
	pID() id
	arity() int
}

// DistinctPred can be embedded as an anonymous field in a struct T, enabling
// *T to be used as a Pred. 
type DistinctPred struct {
	Arity int  // the arity of the predicate
}

func (p * DistinctPred) pID() id {
	return reflect.ValueOf(p).Pointer()
}

func (p * DistinctPred) arity() int {
	return p.A
}

// DBPred holds a predicate that is defined by a database of facts and rules.
type DBPred struct {
	db []*Clause
	DistinctPred
}

// Assert introduces a clause into the relevant database. The head predicate
// must be a DBPred, otherwise an error is returned. The clause must be safe.
func (c *Clause) Assert() error {
	if !c.Safe() {
		return errors.New("datalog: can't assert unsafe clause")
	}
	p, ok := c.Head.Pred.(*DBPred)
	if !ok {
		return errors.New("datalog: can't modify primitive predicate")
	}
	p.db = append(p.db, c)
	return nil
}

// tag returns a "variant tag" for a clause, such that two clauses have the
// same variant tag if and only if they are identical modulo variable renaming.
func (c *Clause) tag() string {
	var buf bytes.Buffer
	varMap := make(map[id]int)
	c.Head.tagf(&buf, varMap)
	for _, literal := range c.Body {
		literal.tagf(&buf, varMap)
	}
	return buf.String()
}

// Retract removes a clause from the relevant database, along with all
// structurally identical clauses modulo variable renaming. The head predicate
// must be a DBPred, otherwise an error is returned.
func (c *Clause) Retract() error {
	p, ok := c.Head.Pred.(*DBPred)
	if !ok {
		return errors.New("datalog: can't modify primitive predicate")
	}
	tag := c.tag()
	for i := 0; i < len(p.db); i++ {
		if p.db[i].tag() == tag {
			n := len(p.db)
			p.db[i], p.db[n-1], p.db = p.db[n-1], nil, p.db[:n-1]
			i--
		}
	}
	return nil
}

// Answers to a query are facts.
type Answers []*Literal

// Query returns a list of facts that unify with the given literal.
func (l *Literal) Query() Answers {
	sg := make(query).search(l)
	if len(sg.facts) == 0 {
		return nil
	}
	a := make(Answers, len(sg.facts))
	i := 0
	for _, fact := range subgoal.facts {
		a[i] = fact
		i++
	}
	return a
}

// RetractOne removes one instance of a clause from the relevant database, or
// one structurally identical clause modulo variable renaming. The head
// predicate must be a DBPred, otherwise an error is returned. If no matching
// clause is found, an error is returned.
func (c *Clause) RetractOne() error {
	p, ok := c.Head.Pred.(*DBPred)
	if !ok {
		return errors.New("datalog: can't modify primitive predicate")
	}
	bodyLen := len(c.Body)  // check body len to avoid some tag calculations
	tag := c.tag()
	for i := 0; i < len(p.db); i++ {
		if len(p.db[i].Body) == bodyLen && p.db[i].tag() == tag {
			n := len(p.db)
			p.db[i], p.db[n-1], p.db = p.db[n-1], nil, p.db[:n-1]
			return nil
		}
	}
	return errors.New("datalog: retract found no matching clauses")
}

// An env maps variables to terms. It is used for substitutions.
type env map[Var]Term

// subst creates a new literal by applying env.
func (l *Literal) subst(env env) *Literal {
	if env == nil || len(env) == 0 || len(l.Arg) == 0 {
		return l
	}
	s := &Literal{Pred: l.Pred, Arg: make([]Term, len(l.Arg))}
	copy(s.Arg, l.Arg)
	for i, arg := range l.Arg {
		if v, ok := arg.(Var); ok {
			if t, ok := env[v]; ok {
				s.Arg[i] = t
			}
		}
	}
	return s
}

// shuffle extends env by adding, for each unmapped variable in the literal's
// arguments, a mappings to a fresh variable. If env is nil, a new environment
// is created.
func (l *Literal) shuffle(env env) env {
	if env == nil {
		env = make(env)
	}
	for _, arg := range l.Arg {
		if v, ok := arg.(*Variable); ok {
			if _, ok := env[v]; !ok {
				env[v] = &DistinctVar{}
			}
		}
	}
	return env
}

// rename generates a new literal by renaming all variables to fresh ones.
func (l *Literal) rename() *Literal {
	return l.subst(l.shuffle(nil))
}

// chase applies env until a constant or an unmapped variable is reached.
func (c *DistinctConst) chase(env env) Term {
	return c
}

// chase applies env until a constant or an unmapped variable is reached.
func (v *DistinctVar) chase(env env) Term {
	if t, ok := env[v]; ok {
		return t.chase(env)
	} else {
		return v
	}
}

// unify const unknown reverses params.
func (c *Const) unify(other Term, env env) env {
	return other.unifyConst(c, env)
}

// unify var unknown reverses params.
func (v *Var) unify(other Term, env env) env {
	return other.unifyVar(v, env)
}

// unify const const fails.
func (c *Const) unifyConst(c2 *Const, env env) env {
	return nil
}

// unify const var maps var to const.
func (c *Const) unifyVar(v *Var, env env) env {
	env[v] = c
	return env
}

// unify var const maps var to const.
func (v *Var) unifyConst(c *Const, env env) env {
	env[v] = c
	return env
}

// unify var var maps var to var.
func (v *Var) unifyVar(v2 *Var, env env) env {
	env[v2] = v
	return env
}

// unify attempts to unify two literals. It returns an environment such that
// a.subst(env) is structurally identical to b.subst(env), or nil if no such
// environment is possible.
func unify(a, b *Literal) env {
	if a.Pred != b.Pred {
		return nil
	}
	env := make(env)
	for i, _ := range a.Arg {
		a_i := a.Arg[i].chase(env)
		b_i := b.Arg[i].chase(env)
		if a_i != b_i {
			env = a_i.unify(b_i, env)
			if env == nil {
				return nil
			}
		}
	}
	return env
}

// drop creates a new clause by dropping d leading parts from the body, then
// applying env to head and to each remaining body part.
func (c *Clause) drop(d int, env env) *Clause {
	n := len(c.Body) - d
	if n < 0 {
		panic("not reached?")
	}
	s := &Clause{
		Head: c.Head.subst(env),
		Body: make([]*Literal, n)
	}
	for i := 0; i < n; i++ {
		s.Body[i] = c.Body[i+d].subst(env)
	}
	return s
}

// subst creates a new clause by applying env to head and to each body part
func (c *Clause) subst(env env) *Clause {
	if env == nil || len(env) == 0 {
		return c
	}
	return c.drop(0, env)
}

// rename generates a new clause by renaming all variables to freshly created
// variables.
func (c *Clause) rename() *Clause {
	// Note: since all variables in head are also in body, we can ignore head
	// while generating the environment.
	var env env
	for _, e := range c.Body {
		env = e.shuffle(env)
	}
	return c.subst(env)
}

// hasVar checks if v appears in a litteral.
func (l *Literal) hasVar(v Var) bool {
	for _, arg := range l.Arg {
		if v == arg {
			return true
		}
	}
	return false
}

// Safe checks whether a clause is safe, that is, whether every variable in the
// head also appears in the body.
func (c *Clause) Safe() bool {
	for _, arg := range c.Head.Arg {
		if v, ok := arg.(*Variable); ok {
			safe := false
			for _, literal := range c.Body {
				if literal.hasVar(v) {
					safe = true
					break
				}
			}
			if !safe {
				return false
			}
		}
	}
	return true
}

// The remainder of this file implements the datalog prover.

// query tracks a set of subgoals, indexed by subgoal target tag.
type query map[string]*subgoal

// newSubgoal creates a new subgoal and adds it to the query's subgoal set.
func (q query) newSubgoal(target *Literal, waiters []*waiter) *subgoal {
	sg := &subgoal{target, make(factSet), waiters}
	q[target.tag()] = sg
	return sg
}

// findSubgoal returns the appropriate subgoal from the query's subgoal set.
func (q query) findSubgoal(target *Literal) *subgoal {
	return q[target.tag()]
}

// factSet tracks a set of literals, indexed by identity tag.
// TODO(kwalsh) This map here and the fact() function below implement a quick way
// to filter out identical literals by relying on Literal.ID().
type factSet map[string]*Literal

type subgoal struct {
	target  *Literal  // e.g. ancestor(X, Y)
	facts   factSet   // facts that unify with literal, e.g. ancestor(alice, bob)
	waiters []*waiter // ?
}

// waiter is a pair containing a subgoal and a rule.
type waiter struct {
	subgoal *subgoal
	rule  *Clause
}

// search introduces a new subgoal for target, with waiters to be notified upon
// discovery of new facts that unify with target.
// Example target: ancestor(X, Y)
func (q query) search(target *Literal, waiters ...*waiter) *subgoal {
	sg := newSubgoal(target, waiters)
	pred, ok := target.Pred.(*predicate)
	if !ok {
		panic("datalog: primitives not yet implemented")
	} else {
		// Examine each fact or rule clause in the relevant database ...
		// Example fact: ancestor(alice, bob)
		// Example rule: ancestor(P, Q) :- parent(P, Q)
		for _, clause := range pred.db {
			// ... and try to unify target with that clause's head.
			renamed := clause.rename()
			env := unify(target, renamed.Head)
			if env != nil {
				// Upon success, process the new discovery.
				q.discovered(sg, renamed.subst(env))
			}
		}
	}
	return subgoal
}

// discovered kicks off processing upon discovery of a fact or rule clause
// whose head unifies with a subgoal target.
func (q query) discovered(sg *subgoal, clause *Clause) {
	if len(clause.Body) == 0 {
		q.discoveredFact(sg, clause.Head)
	} else {
		q.discoveredRule(sg, clause)
	}
}

// discoveredRule kicks off processing upon discovery of a rule whose head
// unifies with a subgoal target.
func (q query) discoveredRule(rulesg *subgoal, rule *Clause) {
	bodysg := q.findSubgoal(rule.Body[0])
	if bodysg == nil {
		// Nothing on body[0], so search for it, but resume processing later.
		q.search(rule.Body[0], &waiter{rulesg, rule})
	} else {
		// Work is progress on body[0], so resume processing later...
		bodysg.waiters = append(bodysg.waiters, &waiter{rulesg, rule})
		// ... but also check facts already known to unify with body[0]. For each
		// such fact, check if rule can be simplified using information from fact.
		// If so then we have discovered a new, simpler rule whose head unifies with
		// the rulesg.target.
		var simplifiedRules []*Clause
		for _, fact := range bodysg.facts {
			r := resolve(rule, fact)
			if r != nil {
				simplifiedRules = append(simplifiedRules, r)
			}
		}
		for _, r := range simplifiedRules {
			q.discovered(sg, r)
		}
	}
}

// discoveredRule kicks off processing upon discovery of a fact that unifies
// with a subgoal target.
func (q query) discoveredFact(factsg *subgoal, fact *Literal) {
	// TODO(kwalsh) pretty sure fact has no variables left (it would be unsafe if it
	// did). So fact.ID() == fact.Tag().
	if _, ok := factsg.facts[fact.ID()]; !ok {
		factsg.facts[fact.ID()] = fact
		// Rusume processing: For each deferred (rulesg, rule) pair, check if rule
		// can be simplified using information from fact. If so then we have
		// discovered a new, simpler rule whose head unifies with rulesg.target.
		for _, waiting := range factsg.waiters {
			r := resolve(waiting.rule, fact)
			if r != nil {
				q.discovered(waiting.subgoal, r)
			}
		}
	}
}

// resolve simplifies rule using information from fact. 
// Example rule:    ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z)
// Example fact:    ancestor(alice, bob)
// Simplified rule: ancestor(alice, Z) :- ancestor(bob, Z)
func resolve(rule *Clause, fact *Literal) *Clause {
	n := len(rule.Body)
	if n == 0 {
		panic("not reached?")
		return nil
	}
	// TODO(kwalsh) pretty sure fact has no variables, so renaming isn't needed.
	env := unify(rule.Body[0], fact.rename())
	if env == nil {
		return nil
	}
	return rule.drop(1, env)
}

