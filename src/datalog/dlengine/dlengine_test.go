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

package dlengine

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

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

func setup(t *testing.T, input string, asserts, retracts, queries, errors int) *Engine {
	e := NewEngine()
	e.AddPred(Equals)
	a, r, q, errs := e.Process("test", input)
	if a != asserts || r != retracts || q != queries || errs != errors {
		t.Fatalf("setup process failed: %d %d %d %d\ninput = %s", a, r, q, errs, input)
	}
	return e
}

func TestEngine(t *testing.T) {
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
	setup(t, input, 3, 1, 5, 0)
}

func check(t *testing.T, e *Engine, query string, ans int) {
	a, err := e.Query(query)
	if err != nil {
		t.Fatal(err.Error())
	}
	if len(a) != ans {
		t.Fatalf("expected %d answers, got %d", ans, len(a))
	}
}

func TestEquals(t *testing.T) {
	e := setup(t, "z(X) :- =(X, 0).", 1, 0, 0, 0)
	check(t, e, "z(0)?", 1)
	check(t, e, "z(7)?", 0)
	check(t, e, "z(X)?", 0)

	e = setup(t, "z(X) :- =(X, 0). f(X, Y) :- z(X), =(X, Y).", 2, 0, 0, 0)
	check(t, e, "f(X, Y)?", 1)

	e = setup(t, "z(X) :- =(X, 0). f(X, Y) :- z(Y), =(X, Y).", 2, 0, 0, 0)
	check(t, e, "f(X, Y)?", 1)

	e = setup(t, "e(X, Y) :- =(X, Y).", 1, 0, 0, 0)
	check(t, e, "e(X, Y)?", 0)

	e = setup(t, "old(X) : person(X), age(X, Y), =(Y, 100). person(alice). age(alice, 102).", 3, 0, 0, 0)
	check(t, e, "old(alice)?", 1)
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
		if (len(a) > 0) != qa[i] {
			t.Fatalf("wrong on query %d: %s was %v, should be %v", i, query, a, qa[i])
		} else {
			fmt.Printf("ok\n")
		}
	}

	// Anecdotal benchmark results:
	// go test completes in about 3.4 seconds on my system
	// datalog's interp is about 13.5 seconds with same system, file, and query
}
