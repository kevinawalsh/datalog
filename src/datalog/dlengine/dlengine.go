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

// This code borrows heavily from the lexer design and implementation for the
// template package. See http://golang.org/src/pkg/text/template/parse/parse.go

// Engine for a text-based Datalog interpreter. Also provides pretty-printing
// for datalog literals, predicates, etc.
package dlengine

import (
	"fmt"
	"strconv"
	"bytes"

	"datalog"
)

// Var represents a variable with a name, e.g. X, Y. Name should start with
// uppercase and follow traditional datalog syntax.
type Var struct {
	Name string
	datalog.DistinctVar
}

func (v *NamedVar) String() {
	return v.Name
}

// Quoted represents a quoted string constant, e.g. "Alice", "Hello\nWorld".
type Quoted struct {
	Value string
	datalog.DistinctConst
}

func (q *Quoted) String() string {
	return strconv.Quote(q.Value)
}

// Ident represents a bare identifier constant, e.g. alice, -42. Value should
// start with non-uppercase and follow traditional datalog syntax.
type Ident struct {
	Value string
	datalog.DistinctConst
}

func (i *Ident) String() string {
	return i.Value
}

// Pred represents a database-defined predicate with a name and arity, e.g.
// ancestor/2. Name should start with non-uppercase and follow traditional
// datalog syntax.
type Pred struct {
	Name string
	datalog.DBPred
}

type (p *Pred) String() string {
	return p.Name
}

// NewRule returns a new clause with the given head and body literals.
func NewRule(head *Literal, body ...*Literal) *Clause {
	return &Clause{Head: head, Body: body}
}

// Engine maintains state for the datalog prover. The main task of the engine is
// to map a given piece of text to existing Var, Ident, Quoted, and Pred
// objects. Because go does not provide weak references, reference counting is
// needed to ensure that objects that are no longer used are removed from the
// Engine to be garbage collected.
type Engine struct {
	Term map[string]Term // live variables, constants, and identifiers
	Predicate map[string]Predicate // live predicates 
	refCount map[interface{}]int
}

func NewEngine() *Engine {
	return &Engine{
		Term: make(map[string]Term),
		Predicate: make(map[string]Predicate),
		refCount: make(map[interface{}]int),
	}
}

func (e *Engine) Process(name, input string) (assertions, retractions, queries, errors int) {
	pgm, err := parse(name, input)
	if err != nil {
		errors++
		fmt.Println("datalog: %s", err.Error())
		return
	}
	for _, node := range pgm.nodeList {
		switch node := node.(type) {
		case *actionNode:
			if node.action == actionAssert {
				err = e.assert(node.clause, true)
				assertions++
			} else {
				err = e.retract(node.clause, true)
				retractions++
			}
		case *queryNode:
			err = e.query(node.literal)
			queries++
		default:
				panic("not reached")
		}
		if err != nil {
			fmt.Printf("datalog: %s:%d: %s\n", name, node.Position(), err.Error())
			errors++
		} else {
			fmt.Printf("OK\n")
		}
	}
	return
}

func (e *Engine) Batch(name, input string) (assertions, retractions int, err error) {
	pgm, err := parse(name, input)
	if err != nil {
		return
	}
	for _, node := range pgm.nodeList {
		switch node := node.(type) {
		case *actionNode:
			if node.action == actionAssert {
				err = e.assert(node.clause, false)
				assertions++
			} else {
				err = e.retract(node.clause, false)
				retractions++
			}
		case *queryNode:
			// ignore
		default:
				panic("not reached")
		}
		if err != nil {
			return
		}
	}
	return
}

func (e *Engine) assert(clause *clauseNode, interactive bool) error {
	c := e.recoverClause(clause)
	if interactive {
		fmt.Printf("Assert: %s\n", c)
	}
	err := c.Assert()
	e.track(c, +1)
	return err
}

func (e *Engine) retract(clause *clauseNode, interactive bool) error {
	c := e.recoverClause(clause)
	if interactive {
		fmt.Printf("Retract: %s\n", c)
	}
	err := c.Retract()
	e.track(c, -1)
	return err
}

func (e *Engine) query(literal *literalNode) error {
	l := e.recoverLiteral(literal)
	fmt.Printf("Query: %s\n", l)
	a := l.Query()
	fmt.Println(a)
	return nil
}

func (e *Engine) Assert(assertion string) error {
	pgm, err := parse("assert", assertion)
	if err != nil {
		return err
	}
	if len(pgm.nodeList) != 1 {
		return fmt.Errorf("datalog: expecting one assertion: %s", assertion)
	}
	node, ok := pgm.nodeList[0].(*actionNode)
	if !ok {
		return fmt.Errorf("datalog: expecting assertion: %s", assertion)
	}
	return e.assert(node.clause, false)
}

func (e *Engine) Retract(retraction string) error {
	pgm, err := parse("retract", retraction)
	if err != nil {
		return err
	}
	if len(pgm.nodeList) != 1 {
		return fmt.Errorf("datalog: expecting one retraction: %s", retraction)
	}
	node, ok := pgm.nodeList[0].(*actionNode)
	if !ok {
		return fmt.Errorf("datalog: expecting retraction: %s", retraction)
	}
	return e.retract(node.clause, false)
}

func (e *Engine) Query(query string) (bool, error) {
	pgm, err := parse("query", query)
	if err != nil {
		return false, err
	}
	if len(pgm.nodeList) != 1 {
		return false, fmt.Errorf("datalog: expecting one query: %s", query)
	}
	node, ok := pgm.nodeList[0].(*queryNode)
	if !ok {
		return false, fmt.Errorf("datalog: expecting query: %s", query)
	}
	l := e.recoverLiteral(node.literal)
	supported := l.Query() != nil
	return supported, nil
}

func (e *Engine) recoverClause(clause *clauseNode) *Clause {
	head := e.recoverLiteral(clause.head)
	body := make([]*Literal, len(clause.nodeList))
	for i, node := range clause.nodeList {
		body[i] = e.recoverLiteral(node.(*literalNode))
	}
	return NewRule(head, body...)
}

func (e *Engine) recoverLiteral(literal *literalNode) *Literal {
	name := literal.predsym
	arity := len(literal.nodeList)
	id := name + "/" + strconv.Itoa(arity)
	p, ok := e.Predicate[id]
	if !ok {
		p = NewPredicate(name, arity)
		e.Predicate[id] = p
	}
	arg := make([]Term, arity)
	for i, n := range literal.nodeList {
		leaf := n.(*leafNode)
		t, ok := e.Term[leaf.val]
		if !ok {
			switch n.Type() {
			case nodeIdentifier:
				t = &Constant{leaf.val}
			case nodeString:
				t = &Constant{leaf.val}
			case nodeVariable:
				t = &Variable{leaf.val}
			default:
				panic("not reached")
			}
			e.Term[leaf.val] = t
		}
		arg[i] = t
	}
	return NewLiteral(p, arg...)
}

func (e *Engine) track(c *Clause, inc int) {
	e.trackLiteral(c.Head, inc)
	for _, l := range c.Body {
		e.trackLiteral(l, inc)
	}
}

func (e *Engine) trackLiteral(l *Literal, inc int) {
	e.trackObject(l.Pred, inc)
	for _, t := range l.Arg {
		e.trackObject(t, inc)
	}
}

func (e *Engine) trackObject(obj interface{}, inc int) {
	count, ok := e.refCount[obj]
	if !ok {
		count = 0
	}
	count += inc
	if count <= 0 {
		delete(e.refCount, obj)
	} else {
		e.refCount[obj] = count
	}
}
