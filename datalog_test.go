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

type VarX struct {
	DistinctVar
}

func (v *VarX) String() string {
	return "X"
}

type VarY struct {
	DistinctVar
}

func (v *VarY) String() string {
	return "Y"
}

type ConstFelix struct {
	DistinctConst
}

func (v *ConstFelix) String() string {
	return "felix"
}

type ConstSylvester struct {
	DistinctConst
}

func (v *ConstSylvester) String() string {
	return "sylvester"
}

type PredSame struct {
	DBPred
}

func (v *PredSame) String() string {
	return "same"
}

type PredExists struct {
	DBPred
}

func (v *PredExists) String() string {
	return "exists"
}

func TestAssertRetract(t *testing.T) {
	same := &PredSame{}
	same.SetArity(2)

	exists := &PredExists{}
	exists.SetArity(1)

	felix := &ConstFelix{}
	sylvester := &ConstSylvester{}
	x := &VarX{}
	y := &VarY{}

	// same(X, X) :- same(felix, felix)
	rule := NewClause(NewLiteral(same, x, x), NewLiteral(same, felix, felix))
	if err := rule.Assert(); err == nil {
		t.Fatal("unsafe rule not detected")
	}
	if s := rule.String(); s != "same(X, X) :- same(felix, felix)" {
		t.Fatalf("rule did not print as expected: %s", s)
	}

	// same(felix, X) :- same(X, felix).
	rule = NewClause(NewLiteral(same, felix, x), NewLiteral(same, x, felix))
	if err := rule.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	// same(felix, felix).
	rule = NewClause(NewLiteral(same, felix, felix))
	if err := rule.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	// same(sylvester, sylvester).
	rule = NewClause(NewLiteral(same, sylvester, sylvester))
	if err := rule.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	// same(felix, x)?
	query := NewLiteral(same, felix, x)
	ans := query.Query()
	if s := ans.String(); s != "same(felix, felix)." {
		t.Fatalf("unexpected answer: %s", s)
	}

	// same(x, felix)?
	query = NewLiteral(same, x, felix)
	ans = query.Query()
	if s := ans.String(); s != "same(felix, felix)." {
		t.Fatalf("unexpected answer: %s", s)
	}

	// same(x, x)?
	query = NewLiteral(same, x, x)
	ans = query.Query()
	if s := ans.String(); s != "same(felix, felix).\nsame(sylvester, sylvester).\n" &&
		s != "same(sylvester, sylvester).\nsame(felix, felix).\n" {
		t.Fatalf("unexpected answer: %s", s)
	}

	// same(felix, felix).
	rule = NewClause(NewLiteral(same, felix, felix))
	if err := rule.Retract(); err != nil {
		t.Fatal(err.Error())
	}

	// same(x, felix)?
	query = NewLiteral(same, x, felix)
	ans = query.Query()
	if len(ans) != 0 {
		t.Fatalf("unexpected answer: %s", ans)
	}
	if s := ans.String(); s != "" && s[0] != '%' {
		t.Fatalf("unexpected answer: %s", ans)
	}

	// same(x, x) :- same(y, y), exists(x)
	rule = NewClause(NewLiteral(same, x, x),
		NewLiteral(same, y, y), NewLiteral(exists, x))
	if err := rule.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	// exists(felix).
	rule = NewClause(NewLiteral(exists, felix))
	if err := rule.Assert(); err != nil {
		t.Fatal(err.Error())
	}

	// same(x, x)?
	query = NewLiteral(same, x, x)
	ans = query.Query()
	if len(ans) != 2 {
		t.Fatalf("unexpected answer: %s", ans)
	}

}
