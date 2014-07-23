Datalog
=======

This library implements a [datalog
system](http://www.ccs.neu.edu/home/ramsdell/tools/datalog/) in Go. The library
is split into three packages:

* datalog -- The core datalog types and prover.
* datalog/dlengine -- A text-based intepreter that serves as a front-end to the
  datalog prover.
* datalog/dlprim -- Custom datalog primitives, like the Equals predicate.

Setup
-----

After installing a suitable version of Go, run:

> go get github.org/kevinawalsh/datalog
> go get github.org/kevinawalsh/datalog/dlengine
> go get github.org/kevinawalsh/datalog/dlprin

Documentation
-------------

See the sources.
