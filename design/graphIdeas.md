# Some graph for traversals

Ideas:
  - 3n+1 graph  (only one neighbour, only circles)
  - Regular: Vertices 0..N-1, connect each vertex i with (101*i + 17) mod N
    and (257*i + 19) mod N, etc., parameter degree, use list of primes
  - Small cliques: Vertices 0..N-1, where N is divisible by the clique size c.
    Connect up all vertices 0..c-1 with each other, c..2*c-1 etc.

