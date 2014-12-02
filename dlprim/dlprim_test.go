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

func TestEqualsFail(t *testing.T) {
	e := setup(t, "", 0, 0, 0, 0)
	err := e.Assert("=(1, 0).")
	if err == nil {
		t.Fatal("datalog allowed client to assert 1 = 0.")
	}
	err = e.Retract("=(1, 1)~")
	if err == nil {
		t.Fatal("datalog allowed client to retract 1 = 1.")
	}
}
