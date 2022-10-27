# `feed`: Large scale data loading/processing tests

This document strives to plan a way in which we can test ArangoDB with larger
data sets. The idea is that we find problems earlier than our customers.

## Brainstorming

We basically have to cover the following areas:

 - large amounts of data (TB area)
 - quick loading of data
 - indexes for the data
 - search indexes for the data
 - data large in comparison to memory size
 - observe query and search performance under load (with a special view towards
   search performance when pages are swapped out)
 - cover insert, replace, remove, update and truncate workloads
 - system under memory pressure
 - system under I/O bandwidth pressure
 - system under I/O ops pressure (network attached volumes in particular)
 - system under network bandwidth pressure
 - system under CPU pressure
 - prevent OOM kills
 - check performance of random reads
 - full collection scans
 - full index scans
 - smart graphs
 - disjoint smart graphs
 - satellite collections
 - satellite graphs
 - hybrid smart graphs
 - smart joins
 - one collection import at a time
 - many collection imports concurrently

Operations needed:

 - batch insert with different overwrite modes
 - batch replace with different overwrite modes
 - batch update with different overwrite modes
 - single reads (sequentially)
 - single reads (random access)
 - batch reads (scattered across full data set)
 - graph traversals
 - index searches (quick)
 - index searches (large amounts of results)
 - arangosearch searches (small result set, but random access)
 - arangosearch searches (large result set)
 - collection truncation
 - search index generation after the fact
 - full collection scan (not producing results)
 - full collection scan (producing some results)
 - full colleciton scan (producing many results)
 - full index scan (not producing results)
 - full index scan (producing some results)
 - full index scan (producing many results)
 - graph traversals (depth first, no results)
 - graph traversals (depth first, some results)
 - graph traversals (depth first, many results)
 - graph traversals (breadth first, no results)
 - graph traversals (breadth first, some results)
 - graph traversals (breadth first, many results)
 - create artificial imbalances
 
We need those operations (where it makes sense) for different types of
collections:

  - normal
  - sharded specially
  - smart vertices
  - smart edges
  - satellites
  - with many indexes and without
  - with search indexes and without

It must be possible to devise tests easily, which first import some data,
then run a constant load of queries on that data, and then do a bulk import
or replace operations concurrently, whilst measuring the observed latency
of the queries. We probably want a selection of test cases, each
scalable to adapt to a certain data size and size of available
resources. Then we want a language to express test combinations
(concurrently and sequentially) to cover a wide range of cases.
Data should be artificially created and the load generators must be high
performance and be able to use concurrency.

This tool should cover the single server and cluster case. The reason is
that many issues can be investigated more cheaply and more easily on single
server, but some need cluster. Active failover will be ignored.

Different protocols (HTTP/1 vs. HTTP/2, JSON vs. VelocyPack) should be
selectable. All communications should go via TLS.

Furthermore, we must be able to measure latency and throughput with
meaningful statistical output in the end.


## Design of the test data

In all cases, keys are known beforehand and are specified (if possible).
For example, we can hash integers 1..nr and take a prefix of the hash or
something.

 1. Normal collection (`normal`)
 2. Normal collection with non-standard sharding by some attribute value
    (`shardedspecial`)
 3. Normal graph (vertex collection + edge collection)
    (maybe a binary tree, `binarytree`)
 3. Smart graph (smart vertex collection + smart edge collection)
    (i.e. there is a smart graph attribute and it is put into prefix of _key)
    (`smartgraph`)
 4. Disjoint smart graph (`disjointsmartgraph`)
 5. Satellite collection (`satellite`)
 6. Normal graph with supernodes (`supernode`)
 7. Smart graph with supernodes (`smartsupernode`)
 8. Hybrid smart graphs
 9. Many collections in same database (`manycollections`), has
    additional parameter for the number

## List of test cases

 - Create
 - Write in batches
 - Random read by primary key
 - Random read by index value (unique index)
 - Full collection read
 - Full index read
 - Single writes
 - Replaces in batches
 - Single replace
 - Updates in batches
 - Single update

## Modifiers

Creation:

  - number of shards
  - replication factor
  - number of indexes
  - special indexes (like geo)
  - arangosearch view

Writing:

  - total size of data
  - approx. size in bytes per document
  - parallelism (number of threads/connections used for import)
  - batch size for import
  - potentially specially indexed fields (like geo, or tag lists)
  - size of keys

Reading:

  - ???

## Language to describe test runs

For each use case, there is a `create` command and a `write` command.
Both have generic modifiers, `create` for number of shards and number of
indexes etc., `write` for data size, parallelism, document size, number
of fields and potentially special data like geo.

Input is line based, curly braces allow grouping for parallel execution,
square brackets allow grouping for sequential execution.

```
[
{
  create normal database=xyz indexes=3 numberOfShards=3 replicationFactor=3
  create smartgraph database=g indexes=0 numberOfShards=3 replicationFactor=3
}
{
  write normal dataSize=500G docSize=2000 parallelism=10 fields=3 geo=true
  [
    write smartgraph vertices dataSize=500G docSize=100 parallelism=10 fields=2
    write smartgraph edges dataSize=500G docSize=50 parallelism=10 fields=1
  ]
}
]
```

## Monitoring and measurements

All operations measure latency and throughput for individual operations
and report in the end or even during the run. All report contain the
line number of the test plan for reference.

## Examples


