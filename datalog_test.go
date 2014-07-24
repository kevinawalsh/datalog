// Copyright (c) 2014, Kevin Walsh.  All rights reserved.
//
// This library is free software; you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation; either version 2 of the
// License, or (at your option) any later version.
//
// This library is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc.  51 Franklin St, Fifth Floor, Boston, MA 02110-1301
// USA

package datalog

import (
	"testing"
)

func TestAllTags(t *testing.T) {
	ancestor := new(DBPred)
	ancestor.SetArity(2)

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
}

func TestProver(t *testing.T) {
	ancestor := new(DBPred)
	ancestor.SetArity(2)

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
