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

// Package dlprim provides custom "primitive" datalog predicates, like Equals.
package dlprim

import (
	"errors"

	"github.com/kevinawalsh/datalog"
)

// Equals is a custom predicate for equality checking, defined by these rules:
//   =(X, Y) generates no facts.
//   =(X, c) generates fact =(c, c).
//   =(c, Y) generates fact =(c, c).
//   =(c, c) generates fact =(c, c).
//   =(c1, c2) generates no facts.
var Equals datalog.Pred

func init() {
	eq := new(eqPrim)
	eq.SetArity(2)
	Equals = eq
}

type eqPrim struct {
	datalog.DistinctPred
}

func (eq *eqPrim) String() string {
	return "="
}

func (eq *eqPrim) Assert(c *datalog.Clause) error {
	return errors.New("datalog: can't assert for custom predicates")
}

func (eq *eqPrim) Retract(c *datalog.Clause) error {
	return errors.New("datalog: can't retract for custom predicates")
}

func (eq *eqPrim) Search(target *datalog.Literal, discovered func(c *datalog.Clause)) {
	a := target.Arg[0]
	b := target.Arg[1]
	if a.Variable() && b.Constant() {
		discovered(datalog.NewClause(datalog.NewLiteral(eq, b, b)))
	} else if a.Constant() && b.Variable() {
		discovered(datalog.NewClause(datalog.NewLiteral(eq, a, a)))
	} else if a.Constant() && b.Constant() && a == b {
		discovered(datalog.NewClause(target))
	}
}
