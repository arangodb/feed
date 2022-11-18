# Graphs for Testing

Generating graphs can be performed by hand or using external generators. For this, we need converters between graph formats.

- Idea: store some large important graphs.


## Structured Graphs
- Trees
- Paths
- Cycles
- Matchings
- Grids
- Edgeless
- Stars
- Wheels
- Planar
   - Use plantri, see http://combos.org/plantri.
- Cliques
- Subdivided Cliques
- Complete n-partite
- Realizations of degree sequences


All structured graphs can obtain a parameter "directionType" which says how the edges should be directed.
For example, in a tree, it can be "downwards" (away from the root), "upwards", "bidirected" or "alternating".
For a path "directed", "bidirected" or "alternating",for a clique "transitive" (the transitive closure of a 
path, i.e., the clique is the result of taking a path and adding edges until the edge relation is transitive) 
or "bidirected". Here "alternating" in a path means that one edge is from left to right, the next fromright
to left (such that the targets meet in one vertex), the next again from left to right and so on.

## Semi-structured Graphs
Graphs obtained from structured graphs by introducing some amount of randomness. 
For example, an undirected tree with one additional random edge or a clique with one randomly removed edge.
## Random Graphs
- Regular Random Graphs        
May be difficult to implement, see https://en.wikipedia.org/wiki/Random_regular_graph. 
- Erdős–Rényi: add edges with a given uniform probability 
- planted partition model (see https://en.wikipedia.org/wiki/Stochastic_block_model): vertices are partitioned into communities; two vertices of the same community are connected with probability q and two vertices from different communities wit probability p
- Watts-Strogatz (see https://en.wikipedia.org/wiki/Watts%E2%80%93Strogatz_model): start from a regular ring lattice (vertices are ordered as a ring, connected each vertex with its k neighbors); then rewire each vertex with probability beta to a randomly chosen vertex outside its k-ring-neighborhood
- Barabási–Albert (see https://en.wikipedia.org/wiki/Barab%C3%A1si%E2%80%93Albert_model): start with a small connected graph, connect each new vertex to an existing vertex with probability proportional to the degree of the exiting vertex.


## Graph Operations
- add singleton
- add edge
- disjoint union
- directed union: A+B + all edges from A to B
- bidirected union: A+B + all edges between A and B
- add a (bi)directed matching given two sets of vertices
- lexicographic product: A-B (replace every vertex in A by a copy of B, replace an edge by all possible edges)
- sparse direct product: A, B -> A -s B: V(A -s B) = V(A) \times V(B) plus V (see below for V) and
   - replace a vertex in A by a copy of B: if (x,y) in E(B) and a in V(A), then ((a, x), (a,y)) in E(A -s B), and
   - if (x, y) in A, then add v_{xy} and v_{yx} to V and add edges (v_{xy}, v_{yx}) in V, all ((x,b), v_{xy}) for b in V(B) to V and add all (v_{yx}, (y, b)) for b in V(B) in V.
