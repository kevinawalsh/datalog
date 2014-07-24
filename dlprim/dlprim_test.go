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

package dlprim

import (
	"testing"

	"github.com/kevinawalsh/datalog/dlengine"
)

func setup(t *testing.T, input string, asserts, retracts, queries, errors int) *dlengine.Engine {
	e := dlengine.NewEngine()
	e.AddPred(Equals)
	a, r, q, errs := e.Process("test", input)
	if a != asserts || r != retracts || q != queries || errs != errors {
		t.Fatalf("setup process failed: %d %d %d %d\ninput = %s", a, r, q, errs, input)
	}
	// fmt.Printf("setup: %s\n", input)
	return e
}

func check(t *testing.T, e *dlengine.Engine, query string, ans int) {
	// fmt.Printf("query: %s\n", query)
	a, err := e.Query(query)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(a) != ans {
		t.Fatalf("expected %d answers, got %d: %v", ans, len(a), a)
	}
}

func TestEquals(t *testing.T) {
	e := setup(t, "z(X) :- =(X, 0).", 1, 0, 0, 0)
	check(t, e, "z(0)?", 1)
	check(t, e, "z(7)?", 0)
	check(t, e, "z(X)?", 1)

	e = setup(t, "z(X) :- =(X, 0). f(X, Y) :- z(X), =(X, Y).", 2, 0, 0, 0)
	check(t, e, "f(X, Y)?", 1)

	e = setup(t, "z(X) :- =(X, 0). f(X, Y) :- z(Y), =(X, Y).", 2, 0, 0, 0)
	check(t, e, "f(X, Y)?", 1)

	e = setup(t, "e(X, Y) :- =(X, Y).", 1, 0, 0, 0)
	check(t, e, "e(X, Y)?", 0)

	e = setup(t, `
	old(X) :- person(X), age(X, Y), =(Y, 100).
	person(alice). age(alice, 102).
	person(bob). age(bob, 100).
	person(carol). age(carol, 100).`, 7, 0, 0, 0)
	check(t, e, "old(alice)?", 0)
	check(t, e, "old(bob)?", 1)
	check(t, e, "old(X)?", 2)
}
