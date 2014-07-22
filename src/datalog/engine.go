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

package datalog

// An engine for a text-based Datalog interpreter.

import (
	"fmt"
	"strconv"
)

type Engine struct {
	Term map[string]Term // variables, constants, and identifiers
	Predicate map[string]Predicate // predicates in use
	refcnt map[interface{}]int
}

func NewEngine() *Engine {
	return &Engine{
		Term: make(map[string]Term),
		Predicate: make(map[string]Predicate),
		refcnt: make(map[interface{}]int),
	}
}

func (e *Engine) Process(name, input string) {
	pgm, err := parse(name, input)
	if err != nil {
		fmt.Println("datalog: %s", err.Error())
		return
	}
	for _, node := range pgm.nodeList {
		switch node := node.(type) {
		case *actionNode:
			if node.action == actionAssert {
				err = e.Assert(node.clause)
			} else {
				err = e.Retract(node.clause)
			}
		case *queryNode:
			err = e.Query(node.literal)
		default:
				panic("not reached")
		}
		if err != nil {
			fmt.Printf("datalog: %s:%d: %s\n", name, node.Position(), err.Error())
		} else {
			fmt.Printf("OK\n")
		}
	}
}

func (e *Engine) Assert(clause *clauseNode) error {
	c := e.recoverClause(clause)
	fmt.Printf("Assert: %s\n", c)
	return c.Assert()
}

func (e *Engine) Retract(clause *clauseNode) error {
	c := e.recoverClause(clause)
	fmt.Printf("Retract: %s\n", c)
	return c.Retract()
}

func (e *Engine) Query(literal *literalNode) error {
	l := e.recoverLiteral(literal)
	fmt.Printf("Query: %s\n", l)
	a := l.Query()
	fmt.Println(a)
	return nil
}

func (e *Engine) recoverClause(clause *clauseNode) *Clause {
	head := e.recoverLiteral(clause.head)
	body := make([]*Literal, len(clause.nodeList))
	for i, node := range clause.nodeList {
		body[i] = e.recoverLiteral(node.(*literalNode))
	}
	return NewClause(head, body...)
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
