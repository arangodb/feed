# The `graphgen` Package

## Resulting Graphs

The graphs are represented as objects of the interface type `graphgen.GraphGenerator`
that delivers vertices and edges in two channels. A vertex or an edge is a document
of type `datagen.Doc`. The fields `Index`, `FromIndex` and `ToIndex` are actually 
only used for internal purposes.

Vertices of a graph are indexed by consecutive natural numbers from 0 to some `n-1`.
(They are stored in the above mentioned fields.) The field `Label` describes the 
position of a vertex in the graph. A graph is constructed in a recursive manner
and a label describes its position in the recursion. Furthermore, the label
describes the position of its vertex in the graph at the end of the recursion.
`FromLabel` and `ToLabel` are the corresponding fields of the edges.
We explain the labels in detail below.

## Usage

There are two ways how a graph can be produced by `graphgen`:

- automatically from a `JSON` document with a description of the graph and
- manually by calling the respective function.
  In general, the automatic way should be the preferred one: it managers a complicated
  interplay between parameters that have to be managed by hand otherwise. In simple cases,
  it may be easier and quicker to use the manual interface.

### Automatic Generation via the `JSON` Interface

A graph can be described in a JSON file that currently has the following structure.
Consider this document as an example (from `lexProdUnionTreePathTree.json`):

```json
{
	"lexProduct": [
		{"union": [
			{"tree": {"branchingDegree": 2, "depth": 1, "directionType": "downwards"}},
			{"path": {"length": 3, "directed": true}}
		]},
		{"tree": {"branchingDegree": 2, "depth": 1, "directionType": "upwards"}}
	]
	
}
```

The whole graph is constructed from sub-graphs in a recursive manner. We thus
have to define a tree whose leaves stand for atomic graphs and inner nodes
describe operations that combine the graphs described in the sub-trees. In the current
implementation, inner nodes always have exactly two children. In the above example,
the leaves describe three atomic graphs: a tree of branching degree 2,
depth 1 with edges directed away from the root ("downwards"); a directed path of
length 3 and a tree of branching degree 2, depth 1 with edges directed towards the root.
The first two sub-graphs are combined as a disjoint union. Finally, the disjoint
union and the last tree are combined by an operation called "lexicographic product".

Atomic graphs are atomic only in the sense that their description fits in the JSON node.
They are represented as JSON documents. Inner vertices have keys that define the operation
to be performed and an array of length exactly two with elements that represent the 
sub-graphs.

The **parser** can be called with 
```go
gg, err := graphgen.JSON2Graph(buffer, flagVertices, flagEdges)
```
where `buffer`, a variable of type `[]byte`, should contain the JSON document,
`flagVertices` is a boolean saying whether to produce the vertices of the graph
and `flagEdges` is a similar boolean for the edges. The function returns a pair
`gg, err` where `gg` is a graph generator and `err` is the thrown error or `nil`.

The graph generator is a variable of the interface type `graphgen.GraphGenerator`:
```go
type GraphGenerator interface {
	VertexChannel() chan *datagen.Doc
	EdgeChannel() chan *datagen.Doc
	NumberVertices() uint64
	NumberEdges() uint64
}
```
Here `VertexChannel()` contains pointers to produced vertices, `EdgeChannel()` pointers
to the edges and `NumberVertices()` and `NumberEdges()` return the corresponding numbers.
It is not necessary to wait for all vertices/edges to be produced to call the latter two
functions. The function `JSON2Graph` starts to fill both channels immediately in parallel
go-routines (except for trees, see below). If `flagVertices` or `flagEdges` is `false`, 
the corresponding channel is empty and immediately closed.

For trees, the procedure is currently different. There is only one go-routine producing
both vertices and/or edges, depending in the flags. Note that if both vertices
and edges are produced but only, say, vertices are consumed, the edge channel gets full
and the whole procedure finds itself in a deadlock. It is the responsibility of the caller
to consume from both channels.

### Nodes of JSON Documents
In the following, "undirected graph" means that the edge relation is symmetric. The 
anti-parallel edges are stored separately. "Undirected edge" means two edges anti-parallel
edges.
#### Leaves
- "cycle"
  - Parameters:
    - "length" of type `uint64`.
  - Labels: the vertices obtain a label prefix that describes its position in the parse tree.
    After that the label contains the number (local index) of the vertex in the cycle.
    Example: "a_b_7" means the vertex is, from the root of the parse tree, at the end of the path
    "root-left child-right child" and, in the cycle, the vertex number 7 (counting from 0).
  
- "path" 
  - Parameters: 
    - "length" (the number of edges) of type `uint64`
    - "directed" of type `bool`. If true, a directed path is created, otherwise an 
    undirected one.
  - Labels: the prefix as above, the local indexes go from the beginning of the path to the 
  end. In a directed path, the beginning is the vertex with in-degree 0, in an undirected one,
  one of the vertices with degree 1.
- "tree"
  - Parameters:
    - "branchingDegree" of type `uint64`: the number of children of each inner vertex (it is the same for all
    inner vertices).
    - "depth" of type `uint64`: the length (the number of edges) of a path from the root to a leaf (it
    is the same for all such paths).
    - "directionType" of type `string`: one of `downwards` (all edges are 
    directed away from the root), `upwards` (all edges are directed to the root),
    `bidirected` (all edges are undirected).
  - Labels: the prefix as above, the rest of the label is a string of the form either
  `eps` (from "epsilon", from "empty", for the root of the tree), or
  `([0-n]-)*[0-n]`, i.e., a sequence of `uint64`-numbers separated by `-`. For example,
  the string "12-2-3" means that the vertex is on a path of length 3 from the root,
  that its first vertex is the 12th child of the root, the second vertex is the 2nd child
  of the first vertex and the third vertex is the 3rd child of the second vertex. Here
  "the 12th", "the 2nd" snd so on refers to an arbitrary but fixed order of children 
  in the produced tree. Note that `eps` does not appear in vertices other than the root.

#### Inner Nodes
- "union"
  - Parameters: no
  - Labels: the vertices of the left sub-tree obtain the prefix `a_`, the vertices of the right 
  sub-tree the prefix `b_`.
- "lexProduct" (see https://en.wikipedia.org/wiki/Lexicographic_product_of_graphs). 
Intuitively, the lexicographic product of two graphs is the first graph where
every vertex is replaced by a copy of the second graph (with its edges) and every 
edge _(a, b)_ of the first graph by all possible edges between all the vertices
replacing _a_ and all the vertices replacing _b_. Note that the number of edges in the 
resulting graph depends quadratically on the number of vertices of the second graph,
so use the lexicographic product with care. The implementation demands that the second
graph is produced before the production of the lexicographic product starts and that
the second graph fits in the memory.
  - Parameters: as for "union".
  - Labels: as for "union".

#### Not Implemented in the Parser
Further types of vertices are "edgeless" (as a leaf) that produces an edgeless graph with 
a given number of vertices and "multiplyEdges" (as an inner node) that has only one
child and replaces its edges by multiple edges.

## Manual Generation

A graph generator can also be produced by manually calling the appropriate function
with appropriate parameters. Examples can be found in the file `graphgen_test.go`.
We explain the general approach on two such examples:

```go
    pathGenerator, err := (&graphgen.PathParameters{
		length, directed,
		graphgen.GeneralParameters{"", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.PathParameters: ", err)
	}
```

Here we create an object of type `graphgen.PathParameters` that obtains
values for the length, the directedness and general parameters. Pointers to
the type `graphgen.PathParameters` implement the interface `Generatable` with
the function `MakeGraphGenerator(makeVertices, makeEdges)` that returns a graph
generator and an error (can be `nil`, of course). `makeVertices` and `makeEdges`
are boolean flags indicating whether the corresponding objects will be produced
(if not, an empty closed channel will be returned by `VertexChannel` or `EdgeCannel()`.)

`GeneralParameters` is used internally in the automatic approach. It
contains `Prefix` (of type `string`), `StartIndexVertices` (of type `uint64`)
and `StartIndexEdges` of type `uint64` that should be computed automatically
when parsing a JSON document and must be given explicitly in this case.
`Prefix` is meant to describe the position of the graph being produced in the
parse tree, `StartIndexVertices` the number of vertices already produced
before (thus the indexes of the graph to produce must start with number `StartIndexVertices`)
and similarly for `StartIndexEdges`. In the above example, the prefix is
empty and no vertices or edges have been produced so far.

```go
    var length uint64 = 2
	var directed bool = true
	lpGenerator, err := (&graphgen.LexicographicalProductParameters{
		&graphgen.PathParameters{length, directed,
			graphgen.GeneralParameters{"a", 0, 0}},
		&graphgen.PathParameters{length, directed,
			graphgen.GeneralParameters{"b", length + 1, length}},
		graphgen.GeneralParameters{"", 0, 0}}).MakeGraphGenerator(true, true)
	if err != nil {
		t.Error("Error in graphgen.LexicographicalProductParameters or in graphgen.PathParameters: ", err)
	}
```
In this example, the lexicographic product sends prefix `a` to its left child
(the first path) and `b` to the second. The indexes of the left child start with 0
for vertices and for edges and the indexes of the right path start with `length+1`
for the vertices (we produce so many vertices in the left path) and with
`length` for the edges (we produce so many edges in the left path). The whole
graph, the lexicographic product, does not have any prefix and its vertices and edges
are indexed starting at 0.
