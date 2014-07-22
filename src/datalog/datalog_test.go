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
	"bufio"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

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
	if a == nil {
		t.Fatal("query failed")
	}
}

func TestLexer(t *testing.T) {
	l := lex("test", "ancestor(X, Z) :- ancestor(X, Y), ancestor(Y, Z).\n"+
		"ancestor(alice, bob).\n"+
		"ancestor(X, Y)?\n")
	for {
		item := l.nextToken()
		// fmt.Println(item)
		if item.typ == itemError {
			t.Fatal("lex error: %v", item)
		}
		if item.typ == itemEOF {
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
	if node == nil {
		t.Fatal("missing parse node")
	}
	// fmt.Println(node)
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
		ancestor(bob, carol)~
		ancestor(alice, carol)?
		`
	a, r, q, errs := e.Process("test", input)
	if a != 3 || r != 1 || q != 5 || errs != 0 {
		t.Fatalf("Process failed: %d %d %d %d", a, r, q, errs)
	}
}

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

func TestPath(t *testing.T) {
	// rng := rand.New(rand.NewSource(1))

	filename := "test.dl"

	n := 100
	e := 200
	f, err := os.Create(filename)
	if err != nil {
		t.Fatal(err.Error())
	}
	out := bufio.NewWriter(f)
	fmt.Fprintf(out, "%% datalog path-finding test\n")
	fmt.Fprintf(out, "%% n = %d vertices\n", n)
	fmt.Fprintf(out, "%% e = %d directed edges\n", e)
	fmt.Fprintf(out, "path(X, Y) :- edge(X, Y).\n")
	fmt.Fprintf(out, "path(X, Z) :- path(X, Y), path(Y, Z).\n")

	g := &graph{n, e, make([]vertex, n)}
	for i := 0; i < e; i++ {
		x := rand.Intn(n)
		y := rand.Intn(n)
		fmt.Fprintf(out, "edge(v-%d, v-%d).\n", x, y)
		g.v[x] = append(g.v[x], y)
	}
	out.Flush()
	f.Close()

	input, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err.Error())
	}

	trials := 5
	qx := make([]int, trials)
	qy := make([]int, trials)
	qa := make([]bool, trials)

	fmt.Printf("generating %d trials\n", trials)
	pos := 0
	for i := 0; i < trials; i++ {
		qx[i] = rand.Intn(n)
		qy[i] = rand.Intn(n)
		qa[i] = path(g, qx[i], qy[i])
		if qa[i] {
			pos++
		}
	}
	fmt.Printf("%d positive trials, %d negative trials\n", pos, trials-pos)

	fmt.Printf("loading database\n")
	engine := NewEngine()
	a, r, err := engine.Batch(filename, string(input))
	if err != nil {
		t.Fatal(err.Error())
	}
	fmt.Printf("loaded %d assertions, %d retractions\n", a, r)
	fmt.Printf("querying database for %d trials\n", trials)
	for i := 0; i < trials; i++ {
		query := fmt.Sprintf("path(v-%d, v-%d)?", qx[i], qy[i])
		fmt.Printf("query %s should be %v\n", query, qa[i])
		a, err := engine.Query(query)
		if err != nil {
			t.Fatal(err.Error())
		}
		if a != qa[i] {
			t.Fatalf("wrong on query %d: %s was %v, should be %v", i, query, a, qa[i])
		}
	}

	// Anecdotal benchmark results:
	// go test completes in about 3.4 seconds on my system
	// datalog's interp is about 13.5 seconds with same system, file, and query
}
