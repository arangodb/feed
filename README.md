# feed - ArangoDB random data and load generator

This project is about putting data and load into and on an ArangoDB
instance. Its purpose is to cover:

 - large data generation
 - quick data import
 - high parallel load of write and read operations
 - multiple different collection types and graphs (smart, hybrid, satellite)
 - different data scenarios (large/small documents, few/many indexes,
   search yes/no)
 - different write scenarios (insert/replace/update/remove/truncate)
 - different read scenarios (bulk read/random read/index read/search read)
 - graph traversals
 - all is covered by a domain specific language which covers parallel
   and sequential loads
 - all operations automatically measure throughput and latency and report

## Example

```
[
normal create database=xyz collection=c numberOfShards=3 replicationFactor=3 drop=true
normal insert database=xyz collection=c parallelism=10
]
```

first creates a normal collection and then inserts some data into it
with 10 parallel go-routines (threads).


## Command line options

Here is the usage page of the tool. Endpoints can be of type `http` or
`https`, they can be separated by commas or the option can be given
multiple times, or both. The endpoints should be different coordinator
endpoints of the same cluster.

```
The 'feed' tool feeds ArangoDB with generated data, quickly.

Usage:
   [flags]

Flags:
      --endpoints strings      Endpoint of server where data should be written. (default [http://localhost:8529])
      --execute string         Filename of program to execute. (default "doit.feed")
  -h, --help                   help for this command
      --jsonOutputFile string  File name of JSON report on performance which is written.
      --jwt string             Verbose output
      --metricsPort int        Metrics port (0 for no metrics) (default 8888)
      --password string        Password for database access
      --protocol string        Protocol (http1, http2, vst) (default "vst")
      --username string        User name for database access. (default "root")
  -v, --verbose                Verbose output
```


## Misc cases

 - `wait`: A no-op waiting program.


## Data cases

This is only an overview, see below for detailed instructions for lists
of subcommands and then later for detailed instructions.

 - `normal`: This is a normal (vertex) collection.
 - `graph`: This is for graphs.

### Operation cases for `normal`

This is only an overview, see below for detailed instructions for each
subcommand.

 - `create`: create collection or graph
 - `drop`: drop a collection
 - `truncate`: trunacte a collection
 - `insert`: bulk insert data
 - `createIdx`: create an index
 - `dropIdx`: drop an index
 - `randomRead`: read documents randomly in parallel
 - `randomUpdate`: perform updates in documents randomly in parallel
 - `randomReplace`: perform replacements of documents in parallel
 - `dropDatabase`: drop a database

### Operation cases for `graph`

This is only an overview, see below for detailed instructions for each
subcommand.

 - `insertvertices`: for graph cases, insert vertex data
 - `insertedges`: for graph cases, insert edge data


## `feedlang` reference

Rules:

 - Input is line based.
 - Curly braces allow grouping for parallel execution.
 - Square brackets allow grouping for sequential execution.
 - Braces and brackets need to be on a line by themselves.
 - White space at the end and the beginning of a line are ignored.
 - Lines in which the first non-white space character is `#` are comments.
 - Empty lines are ignored.

Example:

```
# Comment

[
  {
    wait 1
    wait 2
    wait 3
  }
  {
    wait 4
    wait 3
  }
]
```

This executes the first three `wait` statements concurrently, and when
all three are done (i.e. after 3 seconds), moves to the last two, which
are also executed concurrently (i.e. they end after another 4 seconds).


## Subcommand `create` (for `normal`)

Creates a collection.

Example of usage:

```
normal create database=xyz collection=c numberOfShards=3 replicationFactor=2
```

Possible parameters for usage:

 - `database`: name of the database where the collection will be created
   (default: `_system`)
 - `collection`: name of the collection to be created
   (default: `batchimport`)
 - `numberOfShards`: number of shards of the collection (default: `3`)
 - `replicationFactor`: replication factor (number of replicas for each
   shard) (default: `3`)


## Subcommand `drop` (for `normal`)

Drops a collection.

Example of usage:

```
normal drop database=xyz collection=c
```

Possible parameters for usage:

 - `database`: name of the database where the collection will be dropped
   (default: `_system`)
 - `collection`: name of the collection which will be dropped
   (default: `batchimport`)


## Subcommand `truncate` (for `normal`)

Truncates a collection.

Example of usage:

```
normal truncate database=xyz collection=c
```

Possible parameters for usage:

 - `database`: name of the database where the collection is truncated
   (default: `_system`)
 - `collection`: name of the collection which is truncated
   (default: `batchimport`)


## Subcommand `dropDatabase` (for `normal`)

Drops a database.

Example of usage:

```
normal dropDatabase database=xyz
```

Possible parameters for usage:

- `database`: name of the database which will be dropped


## Subcommand `createIdx` (for `normal`)

Creates an index.

Example of usage:

```
normal createIdx database=xyz collection=c withGeo=false numberFields=4 idxName="myIdx"
```

Possible parameters for usage:

 - `database`: name of the database where the index will be created
   (default: `_system`)
 - `collection`: name of the collection where the index will be created
   (default: `batchimport`)
 - `withGeo`: whether or not it's a geo index (default: `false`)
 - `numberFields`: number of fields the index will cover (default: `1`)
 - `idxName`: user-defined name for the index. Leading and trailing
   quotes are ignored. (default: `"idx" + random number ex. idx123`)


## Subcommand `dropIdx` (for `normal`)

Drops an index.

Example of usage:

`normal dropIdx database=xyz collection=c idxName=myIdx`

Possible parameters for usage:

- `database`: name of the database where the index will be dropped (default: `_system`)
- `collection`: name of the collection where the index will be dropped (default: `batchimport`)
- `idxName`: user-defined name for the index. Leading and trailing quotes are ignored.


## Subcommand `queryOnIdx` (for `normal`)

Runs queries that will use a specific index in parallel, meaning the
query will perform an operation that uses an attribute that is covered
by the index so that it is actively used during the query execution.

Example of query: 

```
FOR doc IN c 
  SORT c.attrCoveredByIdx
  FILTER c.attrCoveredByIdx >= 0
  LIMIT 1
  RETURN doc
```

Example of usage:

```
normal queryOnIdx database=xyz collection=c limit=1 parallelism=1 idxName=primary
```

Possible parameters for usage:

 - `database`: name of the database where the index queries will be
   executed (default: `_system`)
 - `collection`: name of the collection where the index queries will be
   executed (default: `batchimport`)
 - `idxName`: index name. If set to `primary`, the primary index will be
   used on the query. If it's a string, the queries will be performed on
   an attribute of the index with the specified name. If not present, the
   query will be performed in the first index that is not primary that was
   looked up form the collection's indexes.
 - `parallelism`: number of threads to execute the queries concurrently
   (default: `16`)
 - `loadPerThread`: number of times each thread executes the query on
   the same index (default: `50`)
 - `queryLimit`: number of documents returned from the query (default: `1`),
   this controls whether it is a single document or a batch request.


## Subcommand `randomRead` (for `normal`)

Reads single documents randomly in parallel.

Example of usage:

`normal randomRead database=xyz collection=c parallelism=10 loadPerThread=50`

Possible parameters for usage:

 - `database`: name of the database where random reads will be executed
   (default: `_system`)
 - `collection`: name of the collection where the random reads will be
   executed (default: `batchimport`)
 - `parallelism`: number of threads to execute the reads concurrently
   (default: `16`)
 - `loadPerThread`: number of times each thread executes random reads
   (default: `50`)


## Subcommand `randomUpdate` (for `normal`)

Executes updates on random documents.

Example of usage:

```
normal randomUpdate database=xyz collection=c parallelism=10 loadPerThread=50
```

Possible parameters for usage:

 - `database`: name of the database where random updates will be
   executed (default: `_system`)
 - `collection`: name of the collection where the random updates will be
   executed (default: `batchimport`)
 - `parallelism`: number of threads to execute the updates concurrently
   (default: `16`)
 - `loadPerThread`: number of times each thread executes random updates
   (default: `50`)
 - `batchSize`: size of the batches (default: `1000`)


## Subcommand `randomReplace` (for `normal`)

Executes replacements of random documents.

Example of usage:

```
normal randomReplace database=xyz collection=c parallelism=10 loadPerThread=50
```

Possible parameters for usage:

 - `database`: name of the database where random replaces will be
   executed (default: `_system`)
 - `collection`: name of the collection where the random replaces will
   be executed (default: `batchimport`)
 - `parallelism`: number of threads to execute the replaces concurrently
   (default: `16`)
 - `loadPerThread`: number of times each thread executes random replaces
   (default: `50`)
 - `batchSize`: size of the batches (default: `1000`)


## Subcommand `create` (for `graph`)

Creates a graph.

Examples of usage:

```
graph create database=xyz name=G vertexColl=V edgeColl=E type=cyclic graphSize=2000 
```

possible parameters for usage:

 - `database`: name of the database where the graph will be created
   (default: `_system`)
 - `name`: name of the graph to be created (default: `G`)
 - `vertexColl`: name of the vertex collection of the graph (default: `V`)
 - `edgeColl`: name of the edge collection of the graph (default: `E`)
 - `type`: type of graph to be created, currently we have `cyclic` and
   `tree` (default: `cyclic`)
 - `graphSize`: number of vertices of the graph (default: `2000`)
 - `graphDepth`: depth of the created tree (default: `10`)
 - `graphBranching`: branching factor of the tree (default: `2`)
 - `graphDirection`: direction of edges in the tree, can be `downwards`
   or `upwards` or `bidirected` (default: `downwards`)

For the different types of graphs, the following options are relevant:

 - `cyclic`: only needs `graphSize`
 - `tree`: needs `graphDepth`, `graphBranching` and `graphDirection`.


## Subcommand `insertvertices` (for `graph`)

Batch inserts the vertices of a graph.

Examples of usage:

```
graph insertvertices database=xyz name=G vertexColl=V type=cyclic graphSize=2000 
```

possible parameters for usage, see `create`, plus:

 - `batchSize`: size of the batches for the insert (default: `1000`)
 - `parallelism`: number of threads (go-routines) to use client-side
   (default: `16`)


## Subcommand `insertedges` (for `graph`)

Batch inserts the edges of a graph.

Examples of usage:

```
graph insertedges database=xyz name=G edgeColl=E type=cyclic graphSize=2000 
```

possible parameters for usage, see `create`, plus:

 - `batchSize`: size of the batches for the insert (default: `1000`)
 - `parallelism`: number of threads (go-routines) to use client-side
   (default: `16`)

