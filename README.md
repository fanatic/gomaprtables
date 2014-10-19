# MapR Table Go Client

MapR Tables is an implementation of the HBase API.  This client is an extension of https://github.com/mapr/libhbase which is an asyncronous HBase client with some MapR-specific exceptions (like CLDB nodes instead of Zookeeper nodes on connection).

## Compiling & Running
```bash
$ yum install mapr-client
$ export LD_LIBRARY_PATH=/opt/mapr/lib:/usr/lib/jvm/java-1.7.0/jre/lib/amd64/server
$ go build/run
```

## Example App
```bash
example $ go run main.go 
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-1"
    cell[0] [R]: "row-1", [F:Q]: "Id":"i", [V]: "id-1", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-1", [F:Q]: "Name":"First", [V]: "first-1", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-0"
    cell[0] [R]: "row-0", [F:Q]: "Id":"i", [V]: "id-0", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-0", [F:Q]: "Name":"First", [V]: "first-0", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-1"
    cell[0] [R]: "row-1", [F:Q]: "Id":"i", [V]: "id-1", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-1", [F:Q]: "Name":"First", [V]: "first-1", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-2"
    cell[0] [R]: "row-2", [F:Q]: "Id":"i", [V]: "id-2", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-2", [F:Q]: "Name":"First", [V]: "first-2", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-3"
    cell[0] [R]: "row-3", [F:Q]: "Id":"i", [V]: "id-3", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-3", [F:Q]: "Name":"First", [V]: "first-3", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-4"
    cell[0] [R]: "row-4", [F:Q]: "Id":"i", [V]: "id-4", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-4", [F:Q]: "Name":"First", [V]: "first-4", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-5"
    cell[0] [R]: "row-5", [F:Q]: "Id":"i", [V]: "id-5", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-5", [F:Q]: "Name":"First", [V]: "first-5", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-6"
    cell[0] [R]: "row-6", [F:Q]: "Id":"i", [V]: "id-6", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-6", [F:Q]: "Name":"First", [V]: "first-6", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-7"
    cell[0] [R]: "row-7", [F:Q]: "Id":"i", [V]: "id-7", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-7", [F:Q]: "Name":"First", [V]: "first-7", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-8"
    cell[0] [R]: "row-8", [F:Q]: "Id":"i", [V]: "id-8", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-8", [F:Q]: "Name":"First", [V]: "first-8", [TS]: 1969-12-31 19:00:00 -0500 EST
  Table: /tables/jptest  NameSpace: <nil> CellCount: 2 RowKey: "row-9"
    cell[0] [R]: "row-9", [F:Q]: "Id":"i", [V]: "id-9", [TS]: 1969-12-31 19:00:00 -0500 EST
    cell[1] [R]: "row-9", [F:Q]: "Name":"First", [V]: "first-9", [TS]: 1969-12-31 19:00:00 -0500 EST
```