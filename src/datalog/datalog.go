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
	return id(reflect.ValueOf(p).Pointer())
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
	return id(reflect.ValueOf(p).Pointer())
}

// Term represents an argument of a literal. Var and Const implement Term.
type Term interface {
	unify(other Term, e env) env
	unifyVar(other Var, e env) env
	unifyConst(other Const, e env) env
	chase(e env) Term
}

// Literal represents a predicate with terms for arguments. Typical examples
// include person(alice), ancestor(alice, bob), and ancestor(eve, X).
type Literal struct {
	Pred Pred
	Arg []Term
	cachedTag  *string
	cachedID   *string // TODO(kwalsh) remove
}

// NewLiteral returns a new literal with the given predicate and arguments. The
// number of arguments must match the predicate's arity, else panic ensues.
func NewLiteral(p Pred, arg ...Term) *Literal {
	if p.arity() != len(arg) {
		panic("datalog: arity mismatch")
	}
	return &Literal{Pred: p, Arg: arg}
}

// String is a pretty-printer for literals. It produces traditional datalog
// syntax, assuming that all the predicates and terms do when printed with %v.
func (l *Literal) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", l.Pred)
	if len(l.Arg) > 0 {
		fmt.Fprintf(&buf, "(%v", l.Arg[0])
		for i := 1; i < len(l.Arg); i++ {
			fmt.Fprintf(&buf, ", %v", l.Arg[i])
		}
		fmt.Fprintf(&buf, ")")
	}
	return buf.String()
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

// NewClause constructs a new fact (if there are no arguments) or rule
// (otherwise).
func NewClause(head *Literal, body ...*Literal) *Clause {
	return &Clause{Head: head, Body: body}
}

// String is a pretty-printer for clauses. It produces traditional datalog
// syntax, assuming that all the predicates and terms do when printed with %v.
func (c *Clause) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s", c.Head.String())
	if len(c.Body) > 0 {
		fmt.Fprintf(&buf, " :- %s", c.Body[0].String())
		for i := 1; i < len(c.Body); i++ {
			fmt.Fprintf(&buf, ", %s", c.Body[i].String())
		}
	}
	return buf.String()
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
	return id(reflect.ValueOf(p).Pointer())
}

func (p * DistinctPred) arity() int {
	return p.Arity
}

// DBPred holds a predicate that is defined by a database of facts and rules.
type DBPred struct {
	database []*Clause
	DistinctPred
}

// dbPred is trickery to allow dynamic testing for embedded DBPred
// TODO(kwalsh) remove this hack when adding custom predicates
type dbPred interface {
	db() *[]*Clause
}

func (p *DBPred) db() *[]*Clause {
	return &p.database
}

// Assert introduces a clause into the relevant database. The head predicate
// must be a DBPred, otherwise an error is returned. The clause must be safe.
func (c *Clause) Assert() error {
	if !c.Safe() {
		return errors.New("datalog: can't assert unsafe clause")
	}
	p, ok := c.Head.Pred.(dbPred)
	if !ok {
		return errors.New("datalog: can't modify primitive predicate")
	}
	*p.db() = append(*p.db(), c)
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
	p, ok := c.Head.Pred.(dbPred)
	if !ok {
		return errors.New("datalog: can't modify primitive predicate")
	}
	tag := c.tag()
	for i := 0; i < len(*p.db()); i++ {
		if (*p.db())[i].tag() == tag {
			n := len(*p.db())
			(*p.db())[i], (*p.db())[n-1], *p.db() = (*p.db())[n-1], nil, (*p.db())[:n-1]
			i--
		}
	}
	return nil
}

// RetractOne removes one instance of a clause from the relevant database, or
// one structurally identical clause modulo variable renaming. The head
// predicate must be a DBPred, otherwise an error is returned. If no matching
// clause is found, an error is returned.
func (c *Clause) RetractOne() error {
	p, ok := c.Head.Pred.(dbPred)
	if !ok {
		return errors.New("datalog: can't modify primitive predicate")
	}
	bodyLen := len(c.Body)  // check body len to avoid some tag calculations
	tag := c.tag()
	for i := 0; i < len(*p.db()); i++ {
		if len((*p.db())[i].Body) == bodyLen && (*p.db())[i].tag() == tag {
			n := len(*p.db())
			(*p.db())[i], (*p.db())[n-1], *p.db() = (*p.db())[n-1], nil, (*p.db())[:n-1]
			return nil
		}
	}
	return errors.New("datalog: retract found no matching clauses")
}

// Answers to a query are facts.
type Answers []*Literal

// String is a pretty-printer for Answers. It produces traditional datalog
// syntax, assuming that all the predicates and terms do when printed with %v.
func (a Answers) String() string {
	if len(a) == 0 {
		return "% empty"
	} else if len(a) == 1 {
		return a[0].String()
	} else {
		var buf bytes.Buffer
		for _, fact := range a {
			fmt.Fprintf(&buf, "%s\n", fact.String())
		}
		return buf.String()
	}
}

// Query returns a list of facts that unify with the given literal.
func (l *Literal) Query() Answers {
	facts := make(query).search(l).facts
	if len(facts) == 0 {
		return nil
	}
	a := make(Answers, len(facts))
	i := 0
	for _, fact := range facts {
		a[i] = fact
		i++
	}
	return a
}

// An env maps variables to terms. It is used for substitutions.
type env map[Var]Term

// subst creates a new literal by applying env.
func (l *Literal) subst(e env) *Literal {
	if e == nil || len(e) == 0 || len(l.Arg) == 0 {
		return l
	}
	s := &Literal{Pred: l.Pred, Arg: make([]Term, len(l.Arg))}
	copy(s.Arg, l.Arg)
	for i, arg := range l.Arg {
		if v, ok := arg.(Var); ok {
			if t, ok := e[v]; ok {
				s.Arg[i] = t
			}
		}
	}
	return s
}

// shuffle extends env by adding, for each unmapped variable in the literal's
// arguments, a mappings to a fresh variable. If env is nil, a new environment
// is created.
func (l *Literal) shuffle(e env) env {
	if e == nil {
		e = make(env)
	}
	for _, arg := range l.Arg {
		if v, ok := arg.(Var); ok {
			if _, ok := e[v]; !ok {
				e[v] = &DistinctVar{}
			}
		}
	}
	return e
}

// rename generates a new literal by renaming all variables to fresh ones.
func (l *Literal) rename() *Literal {
	return l.subst(l.shuffle(nil))
}

// chase applies env until a constant or an unmapped variable is reached.
func (c *DistinctConst) chase(e env) Term {
	return c
}

// chase applies env until a constant or an unmapped variable is reached.
func (v *DistinctVar) chase(e env) Term {
	if t, ok := e[v]; ok {
		return t.chase(e)
	} else {
		return v
	}
}

// unify const unknown reverses params.
func (c *DistinctConst) unify(other Term, e env) env {
	return other.unifyConst(c, e)
}

// unify var unknown reverses params.
func (v *DistinctVar) unify(other Term, e env) env {
	return other.unifyVar(v, e)
}

// unify const const fails.
func (c *DistinctConst) unifyConst(c2 Const, e env) env {
	return nil
}

// unify const var maps var to const.
func (c *DistinctConst) unifyVar(v Var, e env) env {
	e[v] = c
	return e
}

// unify var const maps var to const.
func (v *DistinctVar) unifyConst(c Const, e env) env {
	e[v] = c
	return e
}

// unify var var maps var to var.
func (v *DistinctVar) unifyVar(v2 Var, e env) env {
	e[v2] = v
	return e
}

// unify attempts to unify two literals. It returns an environment such that
// a.subst(env) is structurally identical to b.subst(env), or nil if no such
// environment is possible.
func unify(a, b *Literal) env {
	if a.Pred != b.Pred {
		return nil
	}
	e := make(env)
	for i, _ := range a.Arg {
		a_i := a.Arg[i].chase(e)
		b_i := b.Arg[i].chase(e)
		if a_i != b_i {
			e = a_i.unify(b_i, e)
			if e == nil {
				return nil
			}
		}
	}
	return e
}

// drop creates a new clause by dropping d leading parts from the body, then
// applying env to head and to each remaining body part. Caller must ensure
// len(c.Body) >= d.
func (c *Clause) drop(d int, e env) *Clause {
	n := len(c.Body) - d
	s := &Clause{
		Head: c.Head.subst(e),
		Body: make([]*Literal, n),
	}
	for i := 0; i < n; i++ {
		s.Body[i] = c.Body[i+d].subst(e)
	}
	return s
}

// subst creates a new clause by applying env to head and to each body part
func (c *Clause) subst(e env) *Clause {
	if e == nil || len(e) == 0 {
		return c
	}
	return c.drop(0, e)
}

// rename generates a new clause by renaming all variables to freshly created
// variables.
func (c *Clause) rename() *Clause {
	// Note: since all variables in head are also in body, we can ignore head
	// while generating the environment.
	var e env
	for _, part := range c.Body {
		e = part.shuffle(e)
	}
	return c.subst(e)
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
		if v, ok := arg.(Var); ok {
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
	facts   factSet   // facts that unify with target, e.g. ancestor(alice, bob)
	waiters []*waiter // waiters such that target unifies with waiter.rule.body[0] 
}

// waiter is a (subgoal, rule) pair, where rule.head unifies with
// subgoal.target.
type waiter struct {
	subgoal *subgoal
	rule  *Clause
}

// search introduces a new subgoal for target, with waiters to be notified upon
// discovery of new facts that unify with target.
// Example target: ancestor(X, Y)
func (q query) search(target *Literal, waiters ...*waiter) *subgoal {
	sg := q.newSubgoal(target, waiters)
	pred, ok := target.Pred.(dbPred)
	if !ok {
		fmt.Println(reflect.TypeOf(target.Pred))
		panic("datalog: primitives not yet implemented")
	} else {
		// Examine each fact or rule clause in the relevant database ...
		// Example fact: ancestor(alice, bob)
		// Example rule: ancestor(P, Q) :- parent(P, Q)
		for _, clause := range *pred.db() {
			// ... and try to unify target with that clause's head.
			renamed := clause.rename()
			e := unify(target, renamed.Head)
			if e != nil {
				// Upon success, process the new discovery.
				q.discovered(sg, renamed.subst(e))
			}
		}
	}
	return sg
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
			q.discovered(rulesg, r)
		}
	}
}

// discoveredRule kicks off processing upon discovery of a fact that unifies
// with a subgoal target.
func (q query) discoveredFact(factsg *subgoal, fact *Literal) {
	// TODO(kwalsh) pretty sure fact has no variables left (it would be unsafe if it
	// did). So fact.ID() == fact.Tag().
	if _, ok := factsg.facts[fact.lID()]; !ok {
		factsg.facts[fact.lID()] = fact
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
		panic("datalog: not reached -- rule can't have empty body")
	}
	// TODO(kwalsh) pretty sure fact has no variables, so renaming isn't needed.
	e := unify(rule.Body[0], fact.rename())
	if e == nil {
		return nil
	}
	return rule.drop(1, e)
}

