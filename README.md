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
