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
	"testing"
)

func TestAllTags(t *testing.T) {
	ancestor := new(DBPred)
	ancestor.Arity = 2

	alice := new(DistinctConst)
	bob := new(DistinctConst)
	carol := new(DistinctConst)

	x := new(DistinctVar)
	y := new(DistinctVar)

	l1 := NewLiteral(ancestor, alice, bob)
	l2 := NewLiteral(ancestor, alice, bob)
	l3 := NewLiteral(ancestor, alice, carol)
	l4 := NewLiteral(ancestor, alice, x)
	l5 := NewLiteral(ancestor, alice, y)

	if l1.tag() != l2.tag() || l4.tag() != l5.tag() {
		t.Fatal("tag mismatch")
	}

	if l1.tag() == l4.tag() || l1.tag() == l3.tag() {
		t.Fatal("false tag match")
	}

	if l1.lID() != l2.lID() {
		t.Fatal("id mismatch")
	}

	if l1.lID() == l3.lID() {
		t.Fatal("false id match")
	}
}

func TestProver(t *testing.T) {
	ancestor := new(DBPred)
	ancestor.Arity = 2

	alice := new(DistinctConst)
	bob := new(DistinctConst)
	carol := new(DistinctConst)

	x := new(DistinctVar)
	y := new(DistinctVar)
	z := new(DistinctVar)

	// ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z)
	rule := NewClause(NewLiteral(ancestor, x, z),
					          NewLiteral(ancestor, x, y), NewLiteral(ancestor, y, z))
	if err := rule.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	fact1 := NewClause(NewLiteral(ancestor, alice, bob))
	if err := fact1.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	fact2 := NewClause(NewLiteral(ancestor, bob, carol))
	if err := fact2.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	ans := NewLiteral(ancestor, x, y).Query()
	if ans == nil {
		t.Fatal("query failed")
	}
	if len(ans) != 3 {
		t.Fatal("query got wrong number of answers")
	}
}

