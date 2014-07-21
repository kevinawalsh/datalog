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

// A Datalog interpreter.
package datalog

import (
	"bytes"
	"errors"
	"strconv"
	"fmt"
)

var _ = fmt.Printf

// A Variable represents a placeholder in datalog.
// Examples: X, Y
// Note: variable names starting with digits are used internally, and should be
// avoided.
type Variable struct {
	Name string
}

var lastFreshCount = 0
func FreshVariable() *Variable {
	lastFreshCount++
	return &Variable{strconv.Itoa(lastFreshCount)}
}

// A Constant represents a concrete value in datalog.
// Examples: alice, bob, "Hello", 42, -3
type Constant struct {
	Value string
}

// A Term appears as the argument of a Literal. A Term can be a Variable or a
// Constant.
// Examples:
//  X        (a Variable)
//  42       (a Constant)
//  "Alice"  (a Constant)
type Term interface {
	isTerm()
	unify(other Term, env Environment) Environment
	unifyVariable(other *Variable, env Environment) Environment
	unifyConstant(other *Constant, env Environment) Environment
}
func (v *Variable) isTerm() {}
func (c *Constant) isTerm() {}

// A Literal is a predicate together with a Term for each argument. The number
// of arguments must match the predicate's arity.
// Examples:
//    ancestor(bob, alice)
//    ancestor(eve, X)
type Literal struct {
	Pred Predicate
	Arg []Term
	tag *string
	id *string
}

func NewLiteral(p Predicate, arg ...Term) *Literal {
	if p.Arity() != len(arg) {
		// TODO(kwalsh) return error?
		return nil;
	}
	return &Literal{Pred: p, Arg: arg}
}

type strpack bytes.Buffer

func (b *strpack) Add(s string) {
	// for debugging, we add some extra braces
	((*bytes.Buffer)(b)).WriteString("[")
	((*bytes.Buffer)(b)).WriteString(strconv.Itoa(len(s)))
	((*bytes.Buffer)(b)).WriteString(":")
	((*bytes.Buffer)(b)).WriteString(s)
	((*bytes.Buffer)(b)).WriteString("]")
}

func (b *strpack) String() string {
	return ((*bytes.Buffer)(b)).String()
}

// Tag returns a "variant tag" for a Literal, such that two Literals have the
// same variant tag if and only if they have are same predicate (both name and
// arity) and the same terms modulo variable renaming.
func (l *Literal) Tag() string {
	if l.tag != nil {
		return *l.tag
	}
	var buf strpack
	buf.Add(PredicateID(l.Pred))
	env := make(map[*Variable]string)
	for i, e := range l.Arg {
		if c, ok := e.(*Constant); ok {
			buf.Add("c" + c.Value)
		} else if v, ok := e.(*Variable); ok {
			tag, ok := env[v]
			if !ok {
				tag = "v" + strconv.Itoa(i)
				env[v] = tag
			}
			buf.Add(tag)
		} else {
			panic("datalog: unrecognized term")
		}
	}
	tag := buf.String()
	l.tag = &tag
	return tag
}


// ID returns an ID for a Literal, such that two Literals have the same ID if
// and only if they have are same predicate (both name and arity) and the same
// terms with identical variable names.
func (l *Literal) ID() string {
	if l.id != nil {
		return *l.id
	}
	var buf strpack
	buf.Add(PredicateID(l.Pred))
	for _, e := range l.Arg {
		if c, ok := e.(*Constant); ok {
			buf.Add("c" + c.Value)
		} else if v, ok := e.(*Variable); ok {
			buf.Add("v" + v.Name)
		} else {
			panic("datalog: unrecognized term")
		}
	}
	id := buf.String()
	l.id = &id
	return id
}

// A Clause has a Head Literal and a Body containing zero or more Literals. With
// an empty body, it is known as a fact. Otherwise, a rule.
// Example fact: parent(alice, bob)
// Example rule: ancestor(A, C) :- ancestor(A, B), ancestor(B, C)
type Clause struct {
	Head *Literal
	Body []*Literal
	id *string
}

func NewClause(head *Literal, body ...*Literal) *Clause {
	return &Clause{Head: head, Body: body}
}

func (c *Clause) Fact() bool {
	return len(c.Body) == 0
}

func (c *Clause) Rule() bool {
	return len(c.Body) > 0
}

// TOOD(kwalsh) Don't know what this is for yet.
func (c *Clause) ID() string {
	if c.id != nil {
		return *c.id
	}
	var buf strpack
	buf.Add(c.Head.ID())
	for _, e := range c.Body {
		buf.Add(e.ID())
	}
	id := buf.String()
	c.id = &id
	return id
}

// A Predicate represents a logical relation, e.g. the "ancestor" relation.
// Every predicate should have a name or arity different from every other
// predicate.
type Predicate interface {
	Name() string // e.g. "ancestor"
	Arity() int // e.g. 2
	Database() []*Clause // e.g. { ancestor(alice, bob), ancestor(X, Y) :- parent(X, Y) }
}

// PredicateID returns a unique ID for a predicate, e.g. "ancestor/2"
func PredicateID(p Predicate) string {
	return p.Name() + "/" + strconv.Itoa(p.Arity())
}

type predicate struct {
	name string
	arity int
	db []*Clause
}
func (p *predicate) Name() string { return p.name }
func (p *predicate) Arity() int { return p.arity }
func (p *predicate) Database() []*Clause { return p.db }

func NewPredicate(name string, arity int) Predicate {
	return &predicate{name, arity, nil}
}

func Assert(c *Clause) error {
	if !c.Safe() {
		return errors.New("datalog: can't assert unsafe clause")
	}
	p, ok := c.Head.Pred.(*predicate)
	if !ok {
		// ignore?
		return errors.New("datalog: can't modify primitive predicate")
	}
	p.db = append(p.db, c)
	return nil
}

func Retract(c *Clause) error {
	p, ok := c.Head.Pred.(*predicate)
	if !ok {
		// ignore?
		return errors.New("datalog: can't modify primitive predicate")
	}
	for i, e := range p.db {
		if e == c {
			n := len(p.db)
			p.db[i] = p.db[n-1]
			p.db = p.db[0:n-1]
			return nil
		}
	}
	return errors.New("datalog: can't retract un-asserted clause")
}

// An Environment maps Variables to Terms. 
type Environment map[*Variable]Term

// subst creates a new literal by mapping variables according to an environment.
func (l *Literal) subst(env Environment) *Literal {
	if env == nil || len(env) == 0 || len(l.Arg) == 0 {
		return l
	}
	s := &Literal{Pred: l.Pred, Arg: make([]Term, len(l.Arg))}
	for i, e := range l.Arg {
		if c, ok := e.(*Constant); ok {
			s.Arg[i] = c
		} else if v, ok := e.(*Variable); ok {
			if t, ok := env[v]; ok {
				s.Arg[i] = t
			} else {
				s.Arg[i] = v
			}
		}
	}
	return s
}

// shuffle extends an environment by adding, for each unmapped variable in the
// literal's arguments, a mappings to a fresh variable. If the environment is
// nil, a new one is created.
func (l *Literal) shuffle(env Environment) Environment {
	if env == nil {
		env = make(Environment)
	}
	for _, e := range l.Arg {
		if v, ok := e.(*Variable); ok {
			if _, ok := env[v]; !ok {
				env[v] = FreshVariable()
			}
		}
	}
	return env
}

// rename generates a new literal by renaming all variables to freshly created
// variables.
func (l *Literal) rename() *Literal {
	return l.subst(l.shuffle(nil))
}

// chase returns a constant or an unbound variable
func chase(t Term, env Environment) Term {
	if c, ok := t.(*Constant); ok {
		return c
	} else if v, ok := t.(*Variable); ok {
		if t, ok := env[v]; ok {
			return chase(t, env)
		} else {
			return v
		}
	} else {
		panic("datalog: unrecognized term")
	}
}

func (c *Constant) unify(other Term, env Environment) Environment {
	return other.unifyConstant(c, env)
}

func (c *Constant) unifyConstant(other *Constant, env Environment) Environment {
	return nil
}

func (v *Variable) unifyConstant(other *Constant, env Environment) Environment {
	env[v] = other
	return env
}

func (v *Variable) unify(other Term, env Environment) Environment {
	return other.unifyVariable(v, env)
}

func (c *Constant) unifyVariable(other *Variable, env Environment) Environment {
	return other.unifyConstant(c, env)
}

func (v *Variable) unifyVariable(other *Variable, env Environment) Environment {
	env[other] = v
	return env
}

// unify attempts to unify two literals. It returns an environment such that
// a.subst(env) is structurally identical to b.subst(env), or nil if no such
// environment is possible.
func unify(a, b *Literal) Environment {
	if a.Pred != b.Pred {
		return nil
	}
	env := make(Environment)
	for i, _ := range a.Arg {
		a_i := chase(a.Arg[i], env)
		b_i := chase(b.Arg[i], env)
		if a_i != b_i {
			env = a_i.unify(b_i, env)
			if env == nil {
				return nil
			}
		}
	}
	return env
}

func (l *Literal) hasTerm(t Term) bool {
	for _, e := range l.Arg {
		if t == e {
			return true
		}
	}
	return false
}

// subst creates a new clause by applying subst to head and each body literal
func (c *Clause) subst(env Environment) *Clause {
	if env == nil || len(env) == 0 {
		return c
	}
	s := &Clause{Head: c.Head.subst(env), Body: make([]*Literal, len(c.Body))}
	for i, e := range c.Body {
		s.Body[i] = e.subst(env)
	}
	return s
}

// rename generates a new clause by renaming all variables to freshly created
// variables.
func (c *Clause) rename() *Clause {
	// Note: since all variables in head are also in body, we can ignore head
	// while generating the environment.
	var env Environment
	for _, e := range c.Body {
		env = e.shuffle(env)
	}
	return c.subst(env)
}

// Safe checks whether a clause is safe, that is, whether every variable in the
// head also appears in the body.
func (c *Clause) Safe() bool {
	for _, e := range c.Head.Arg {
		if v, ok := e.(*Variable); ok {
			safe := false
			for _, e := range c.Body {
				if e.hasTerm(v) {
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

// Unify a literal with a fact that contains only constant terms, or return nil
// if unification fails.
// TODO(kwalsh) second arg is really a clause?
// func (l *Literal) match(fact *Literal) {
// 	env := make(Environment)
// 	// TODO(kwalsh) assumes pred (and arity) matches?
// 	for i, e := range l.Arg {
// 		c := fact.Arg[i]
// 		if e != c {
// 			v, ok := e.(*Variable)
// 			if !ok {
// 				return nil
// 			}
// 			t, ok := env[v]
// 			if !ok {
// 				env[v] = c
// 			} else if t != c {
// 				return nil
// 			}
// 		}
// 	}
// 	return env
// }

var subgoals map[string]*Subgoal

func find(l *Literal) *Subgoal {
	subgoal, _ := subgoals[l.Tag()]
	return subgoal
}

func merge(subgoal *Subgoal) {
	subgoals[subgoal.literal.Tag()] = subgoal
}

// A Subgoal has a literal, a set of facts, and a list of waiters.
type Subgoal struct {
	literal *Literal
	facts []*Literal
	waiters []*Waiter
}

func NewSubgoal(l *Literal) *Subgoal {
	return &Subgoal{l, nil, nil}
}

// A Waiter is a pair containing a subgoal and a clause.
type Waiter struct {
	subgoal Subgoal
	clause Clause
}

