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


	err := Assert(NewClause(l1))
	if err != nil {
		t.Fatal(err.Error())
	}
	err = Assert(NewClause(l5))
	if err != nil {
		t.Fatal(err.Error())
	}
	err = Assert(c)
	if err != nil {
		t.Fatal(err.Error())
	}
	a := Ask(NewLiteral(ancestor, x, y))
	fmt.Println(a)
}

