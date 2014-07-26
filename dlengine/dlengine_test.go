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

package dlengine

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"

	"github.com/kevinawalsh/datalog/dlprim"
)

func runLexer(t *testing.T, l *lexer) token {
	for {
		item := l.nextToken()
		if item.String() == "" {
			return item
		}
		if item.typ == itemError {
			return item
		}
		if item.typ == itemEOF {
			return item
		}
	}
}

func TestLexer(t *testing.T) {
	l := lex("test", `
			ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z).
			ancestor(alice, bob).
			% this is a comment
			ancestor(alice, "bob smith"). % this is another comment
			ancestor(X, Y)?
		`)
	item := runLexer(t, l)
	if item.typ != itemEOF {
		t.Fatalf("unexpected token: %s", item)
	}
}

func TestLexerFail(t *testing.T) {
	l := lex("test", `ancestor(X, Z) :- ancestor`)
	item := runLexer(t, l)
	if item.typ != itemEOF {
		t.Fatalf("unexpected token: %s", item)
	}

	l = lex("test", `ancestor(X, Z) : ancestor(X, Y), ancestor(Y, Z).`)
	item = runLexer(t, l)
	if item.typ != itemError {
		t.Fatalf("unexpected token: %s", item)
	}
	if l.nextToken().typ != itemError {
		t.Fatalf("unexpected token: %s", item)
	}
}

func TestParser(t *testing.T) {
	input := `ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z).
			ancestor(alice, bob).
			ancestor(alice, "bob smith").
			ancestor(X, Y)?`
	node, err := parse("test",  input)
	if err != nil {
		t.Fatal(err.Error())
	}
	if node == nil {
		t.Fatal("missing parse node")
	}
	if s := node.String(); s != input {
		t.Fatalf("bad format: %s\n", s)
	}
}

func setup(t *testing.T, input string, asserts, retracts, queries, errors int) *Engine {
	e := NewEngine()
	a, r, q, errs := e.Process("test", input)
	if a != asserts || r != retracts || q != queries || errs != errors {
		t.Fatalf("setup process failed: %d %d %d %d\ninput = %s", a, r, q, errs, input)
	}
	// fmt.Printf("setup: %s\n", input)
	return e
}

var simpleProgram = `
		ancestor(alice, "bob smith").
		ancestor(X, Y)?
		ancestor("bob smith", carol).
		ancestor(X, Y)?
		ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z).
		ancestor(X, Y)?
		ancestor(X)?
		ancestor("bob smith", carol)~
		ancestor(alice, carol)?
		`

func TestEngine(t *testing.T) {
	setup(t, simpleProgram, 3, 1, 5, 0)
}

func TestBatch(t *testing.T) {
	e := NewEngine()
	a, r, err := e.Batch("test", simpleProgram)
	if err != nil {
		t.Fatal(err.Error())
	}
	if a != 3 || r != 1 {
		t.Fatalf("batch failed: %d %d\n", a, r)
	}
}

func TestEngineErrors(t *testing.T) {
	setup(t, "ancestor(?)", 0, 0, 0, 1)
}

func TestAssert(t *testing.T) {
	e := NewEngine()
	err := e.Assert("same(1, 1).")
	if err != nil {
		t.Fatal(err.Error())
	}
	err = e.Assert("same(1, 1)")
	if err != nil {
		t.Fatal(err.Error())
	}
	err = e.Assert("same(1, 1)?")
	if err == nil {
		t.Fatal("assert with query should be error")
	}
	err = e.Assert("same(1, 1)~")
	if err == nil {
		t.Fatal("assert with retraction should be error")
	}
	err = e.Assert("same(1, 1). same(2, 2).")
	if err == nil {
		t.Fatal("multiple stmts should fail")
	}
}

func TestRetract(t *testing.T) {
	e := NewEngine()
	err := e.Retract("same(1, 1)~")
	if err != nil {
		t.Fatal(err.Error())
	}
	err = e.Retract("same(1, 1)")
	if err != nil {
		t.Fatal(err.Error())
	}
	err = e.Retract("same(1, 1)?")
	if err == nil {
		t.Fatal("retract with query should be error")
	}
	err = e.Retract("same(1, 1).")
	if err == nil {
		t.Fatal("retract with assertion should be error")
	}
	err = e.Retract("same(1, 1)~ same(2, 2)~")
	if err == nil {
		t.Fatal("multiple stmts should fail")
	}
}

func TestQuery(t *testing.T) {
	e := NewEngine()
	_, err := e.Query("same(1, 1)?")
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = e.Query("same(1, 1)")
	if err != nil {
		t.Fatal(err.Error())
	}
	_, err = e.Query("same(1, 1).")
	if err == nil {
		t.Fatal("query for assertion should be error")
	}
	_, err = e.Query("same(1, 1)~")
	if err == nil {
		t.Fatal("query for retraction should be error")
	}
	_, err = e.Query("same(1, 1)? same(2, 2)?")
	if err == nil {
		t.Fatal("multiple stmts should fail")
	}
}

func TestAddPred(t *testing.T) {
	e := NewEngine()
	e.AddPred(dlprim.Equals)
	ans, err := e.Query("=(1, 1)?")
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(ans) != 1 {
		t.Fatal("expecting answer for query =(1, 1) but got nothing")
	}
	err = e.Assert("equals(1, 2)")
	if err != nil {
		t.Fatal(err)
	}
	err = e.Retract("same(1, 1)")
	if err != nil {
		t.Fatal(err)
	}
	err = e.Assert("=(1, 0)")
	if err == nil {
		t.Fatal("datalog allowed client to assert 1 = 0.")
	}
	err = e.Retract("=(1, 1)")
	if err == nil {
		t.Fatal("datalog allowed client to retract 1 = 1.")
	}
	a, r, q, errs := e.Process("bad assertion", "=(1, 0).")
	if a != 1 || r != 0 || q != 0 || errs != 1 {
		t.Fatalf("setup process failed: %d %d %d %d\n", a, r, q, errs)
	}
}

// The remainder of this file is a simiple graph path-finding benchmark.

type vertex []int

type graph struct {
	n int
	e int
	v []vertex
}

func path(g *graph, src, dst int) bool {
	// visit src
	if src == dst {
		return true
	}
	visited := make([]bool, g.n)
	visited[src] = true
	q := make([]int, g.n, g.n)
	n := 0
	q[n] = src
	n++

	for n > 0 {
		n--
		v := q[n]
		for _, a := range g.v[v] {
			if !visited[a] {
				// visit a
				if a == dst {
					return true
				}
				visited[a] = true
				q[n] = a
				n++
			}
		}
	}
	return false
}

func BenchmarkDatalogPathFinding(b *testing.B) {
	// We store
	filename := "test.dl"

	n := 100
	e := 200
	t := 5 * b.N // run 5 queries per benchmark to get a mix of pos/neg results
	f, err := os.Create(filename)
	if err != nil {
		b.Fatal(err.Error())
	}
	out := bufio.NewWriter(f)
	fmt.Fprintf(out, "%% datalog path-finding benchmark\n")
	fmt.Fprintf(out, "%% n = %d vertices\n", n)
	fmt.Fprintf(out, "%% e = %d directed edges\n", e)
	fmt.Fprintf(out, "%% t = %d trials\n", t)

	fmt.Fprintf(out, "path(X, Y) :- edge(X, Y).\n")
	fmt.Fprintf(out, "path(X, Z) :- path(X, Y), path(Y, Z).\n")

	g := &graph{n, e, make([]vertex, n)}
	for i := 0; i < e; i++ {
		x := rand.Intn(n)
		y := rand.Intn(n)
		fmt.Fprintf(out, "edge(v-%d, v-%d).\n", x, y)
		g.v[x] = append(g.v[x], y)
	}

	queries := make([]string, t)
	answers := make([]int, t)

	for i := 0; i < t; i++ {
		x := rand.Intn(n)
		y := rand.Intn(n)
		queries[i] = fmt.Sprintf("path(v-%d, v-%d)?", x, y)
		if path(g, x, y) {
			fmt.Fprintf(out, "%% The following query should produce one response.\n")
			answers[i] = 1
		} else {
			fmt.Fprintf(out, "%% The following query should produce no responses.\n")
			answers[i] = 0
		}
		fmt.Fprintf(out, "%s\n", queries[i])
	}

	out.Flush()
	f.Close()

	// end of setup
	b.ResetTimer()

	input, err := ioutil.ReadFile(filename)
	if err != nil {
		b.Fatal(err.Error())
	}

	fmt.Printf("loading database\n")
	engine := NewEngine()
	a, r, err := engine.Batch(filename, string(input))
	if err != nil {
		b.Fatal(err.Error())
	}
	fmt.Printf("loaded %d assertions, %d retractions\n", a, r)
	fmt.Printf("querying database %d times\n", t)
	for i := 0; i < t; i++ {
		fmt.Printf("query %s should produce %v responses\n", queries[i], answers[i])
		a, err := engine.Query(queries[i])
		if err != nil {
			b.Fatal(err.Error())
		}
		if len(a) != answers[i] {
			b.Fatalf("failed on trial %d: expecting %d answers, got %d answers\n", i, answers[i], len(a))
		} else {
			fmt.Printf("ok\n")
		}
	}

	// Anecdotal benchmark results:
	// go test completes in about 3.4 seconds on my system
	// datalog's interp is about 13.5 seconds with same system, file, and query
}
