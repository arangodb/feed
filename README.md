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
      --endpoints strings   Endpoint of server where data should be written. (default [http://localhost:8529])
      --execute string      Filename of program to execute. (default "prog.feed")
  -h, --help                help for this command
      --jwt string          Verbose output
      --password string     Password for database access.
      --username string     User name for database access. (default "root")
  -v, --verbose             Verbose output
```


## Data cases

 - `normal`: This is a normal collection

   Subcommands:
     - `create` ...
     - `insert` ...

   Available options:
     - ...

## Operation cases

 - `create`: create collection or graph
 - `insert`: bulk insert data
 - `insertvertices`: for graph cases, insert vertex data
 - `insertedges`: for graph cases, insert edge data
 - `createIdx`: create an index
 - `randomRead`: read documents randomly in parallel
 - `randomUpdate`: perform updates in documents randomly in parallel
 - `randomReplace`: perform replacements of documents in parallel

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

## Subcommand `createIdx`
Creates an index.

Example of usage:

`normal createIdx database=xyz collection=c withGeo=false numberFields=4 idxName="myIdx"`

Possible parameters for usage:

- `database`: name of the database where the index will be created (default: `_system`)
- `collection`: name of the collection where the index will be created (default: `batchimport`)
- `withGeo`: whether or not it's a geo index (default: `false`)
- `numberFields`: number of fields the index will cover (default: `1`)
- `idxName`: user-defined name for the index. Leading and trailing quotes are ignored. (default: `"idx" + random number ex. idx123`)

## Subcommand `queryOnIdx`
Runs queries that will use a specific index in parallel, meaning the query will perform an operation that uses an attribute that is covered by the index so that it is actively used during the query execution.

Example of query: `FOR doc IN c SORT c.attrCoveredByIdx FILTER c.attrCoveredByIdx >= 0 LIMIT 1 RETURN doc`

Example of usage:

`normal queryOnIdx database=xyz collection=c limit=1 parallelism=1 idxName=primary`

Possible parameters for usage:
- `database`: name of the database where the index queries will be executed (default: `_system`)
- `collection`: name of the collection where the index queries will be executed (default: `batchimport`)
- `idxName`: index name. If set to `primary`, a primary index will be used on the query. If it's a string, the queries will be performed on an attribute of the index with the specified name. If not present, the query will be performed in the first index that is not primary that was looked up form the collection's indexes. 
- `parallelism`: number of threads to execute the queries concurrently (default: `16`)
- `loadPerThread`: number of times each thread executes the query on the same index (default: `50`)
- `queryLimit`: number of documents returned from the query (default: `1`)

## Subcommand `randomRead`
Reads documents randomly in parallel.

Example of usage:

`normal randomRead database=xyz collection=c parallelism=10 loadPerThread=50`

Possible parameters for usage:

- `database`: name of the database where random reads will be executed (default: `_system`)
- `collection`: name of the collection where the random reads will be executed (default: `batchimport`)
- `parallelism`: number of threads to execute the reads concurrently (default: `16`)
- `loadPerThread`: number of times each thread executes random reads (default: `50`)

## Subcommand `randomRead`
Executes updates on random documents.

Example of usage:

`normal randomUpdate database=xyz collection=c parallelism=10 loadPerThread=50`

Possible parameters for usage:
- `database`: name of the database where random updates will be executed (default: `_system`)
- `collection`: name of the collection where the random updates will be executed (default: `batchimport`)
- `parallelism`: number of threads to execute the updates concurrently (default: `16`)
- `loadPerThread`: number of times each thread executes random updates (default: `50`)

## Subcommand `randomReplace`
Executes replacements of random documents.

Example of usage:

`normal randomUpdate database=xyz collection=c parallelism=10 loadPerThread=50`

Possible parameters for usage:
- `database`: name of the database where random replaces will be executed (default: `_system`)
- `collection`: name of the collection where the random replaces will be executed (default: `batchimport`)
- `parallelism`: number of threads to execute the replaces concurrently (default: `16`)
- `loadPerThread`: number of times each thread executes random replaces (default: `50`)

