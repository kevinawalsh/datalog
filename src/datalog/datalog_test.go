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

package datalog

import (
	"fmt"
	"testing"
)

var _ = fmt.Println

func TestTypes(t *testing.T) {
	ancestor := NewPredicate("ancestor", 2)
	if PredicateID(ancestor) != "ancestor/2" {
		t.Fatal("bad pred id")
	}

	alice := &Constant{"alice"}
	bob := &Constant{"bob"}
	carol := &Constant{"carol"}
	x := &Variable{"X"}
	y := &Variable{"Y"}
	z := &Variable{"Z"}

	l1 := NewLiteral(ancestor, alice, bob)
	l2 := NewLiteral(ancestor, alice, bob)
	l3 := NewLiteral(ancestor, alice, x)
	l4 := NewLiteral(ancestor, alice, y)
	l5 := NewLiteral(ancestor, bob, carol)

	if l1.Tag() != l2.Tag() || l3.Tag() != l4.Tag() {
		t.Fatal("tag mismatch")
	}

	if l1.Tag() == l3.Tag() {
		t.Fatal("false tag match")
	}

	if l1.ID() != l2.ID() {
		t.Fatal("id mismatch")
	}

	if l1.ID() == l3.ID() || l3.ID() == l4.ID() {
		t.Fatal("false id match")
	}

	// ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z)
	h := NewLiteral(ancestor, x, z)
	b1 := NewLiteral(ancestor, x, y)
	b2 := NewLiteral(ancestor, y, z)
	c := NewClause(h, b1, b2)

	err := NewClause(l1).Assert()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = NewClause(l5).Assert()
	if err != nil {
		t.Fatal(err.Error())
	}
	err = c.Assert()
	if err != nil {
		t.Fatal(err.Error())
	}
	a := NewLiteral(ancestor, x, y).Query()
	fmt.Println(a)
}

func TestLexer(t *testing.T) {
	l := lex("test", "ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z).\n"+
		"ancestor(alice, bob).\n"+
		"ancestor(X, Y)?\n")
	for {
		item := l.nextToken()
		fmt.Println(item)
		if item.typ == itemEOF || item.typ == itemError {
			break
		}
	}
}

func TestParser(t *testing.T) {
	node, err := parse("test", "ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z).\n"+
		"ancestor(alice, bob).\n"+
		"ancestor(X, Y)?\n")
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Println(node)
}

func TestEngine(t *testing.T) {
	e := NewEngine()
	input := `
		ancestor(alice, bob).
		ancestor(X, Y)?
		ancestor(bob, carol).
		ancestor(X, Y)?
		ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z).
		ancestor(X, Y)?
		ancestor(X)?
		`
	e.Process("test", input)
}
