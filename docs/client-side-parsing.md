% Performance improvements in marklogic-rdf4j ingestion
% Charles Greer <mailto:cgreer@marklogic.com>
% 1/10/2018

(c) 2018 MarkLogic Confidential Material


# Status of this Document

Draft Proposal


# Purpose

This document describes a change to the marklogic-rdf4j (and by extrapolation
to marklogic-jena) to improve the performance of RDF triple ingestion.  Current
methods for ingesting triples vary in performance, but the stated overall goal
of our customer is that we should

* be able to ingest 600,000 triples, in a single transaction, in 120 seconds.


# Overview

We actually already meet this goal...  but barely, and in inscrutable ways.
Thus in the interest of general performance and UX improvements for our RDF
client libraries, this specification describes changes to the marklogic-rdf4j
library to to improve performance of RDF ingestion.

To prepare for this work, we've measured the rough performance of several
methods of RDF ingestion.  In each case, the same 600,000 triples are loaded
into MarkLogic from a Java application.   In all cases the Java application and
MarkLogic are on separate machines.  So time includes all time for network and
ingestion.


Source format | Ingestion Method | Thread Count  | REST Endpoint | cwg time | jj time |
---           | ---              | ---           | ---           | ---      | ---     |
turtle        | rdf4j conn.add() | 1             | /v1/graphs    |  51      |  30     |
ntriples      | rdf4j conn.add() | 1             | /v1/graphs    | 135      |         |
parsed triples | conn.add(model) | 1             | /v1/graphs/sparql | 127  |         |
parsed triples | RDFDataMgr.read() | 1           | /v1/graphs/sparql | 426  |         |
6000 XML docs | XMLDocMgr        | 1             | /v1/documents | 116      |         |
6000 XML docs | DataMovementMgr  | 1             | /v1/documents |  77      |         |
6000 XML docs | DataMovementMgr  | 18            | /v1/documents |  35      |         |
6000 XML docs | XCC              | 18            | xcc           |  32      |  6.1    |
turtle        | jena + DMSDK     | 18            | /v1/documents |  48      |         |
ntriples      | jena + DMSDK     | 18            | /v1/documents |  45      |         |
turtle        | jena + DMSDK     | 32            | /v1/documents |  56      |         |
quads?        | jena + DMSDK     | 18            | xcc           |          |  3.5    |


Notes:

* the multithreaded cases in this table are not in a single transaction.
* We speculate that the better improvement for jj's reflects better perf of a cluster
* DMSDK batch size of 200 seems to perform optimally -- big enough payloads, but keeps server load more constanct.
* size of payload makes big difference in turtle vs. ntriples over the wire.
* Note how adding more threads ceases to improve performance at some point.

Generally speaking, we want to accomplish two things with this effort:

1. Refactor client ingestion to manage multi-threaded insert.
1. Refactor client to parse all RDF formats into XML documents each containing 100 triples.

We expect that these changes will take the first four methods above (with times
from 51 to 426 seconds) to times more in the range of the bottom four lines (50
seconds).  Note that the worst case multithreaded case is not much better than
the best case single-threaded method.  Moreover, on a three-node cluster the
benefits of multithreaded ingestion are more directly realized.

These claims must be measured and verified by the performance team.

# Modules affected

## Behavior Changes

### Leveling of performance across method invocations.

The behavior of some functions is changed, but not the implementation.   Here
is a call that uses the RDF4j library to load 600k triples.  The source in this
case is a file containing turtle.

```java
public void rdf4jLoadTurtle() throws IOException {
    MarkLogicRepository repository = new MarkLogicRepository(client());
    repository.initialize();
    MarkLogicRepositoryConnection conn = repository.getConnection();
    conn.add(new File("data/turtletriples/turtle600k.ttl"), "http://example.org/", RDFFormat.TURTLE );
    conn.close();
}
```

In marklogic-jena 1.1.0, this call is implemented by sending the entire
contents of the file, `turtle600k.ttl` to an endpoint in a MarkLogic
application server `/v1/graphs`.  It uses a single thread to send this file,
and it is parsed by MarkLogic into triples and stored in XML documents, 100
triples per XML document.

Note this similar function call:

```java
public void rdf4jLoadTurtle() throws IOException {
    MarkLogicRepository repository = new MarkLogicRepository(client());
    repository.initialize();
    MarkLogicRepositoryConnection conn = repository.getConnection();
    conn.add(new File("data/ntriples/ntriples600k.nt"), "http://example.org/", RDFFormat.NTRIPLE );
    conn.close();
}
```


takes twice as long because the turtle format in this case is compact than the
ntriples by a factor of four.  The amount of data crossing network boundaries
can vary significantly depending on chosen RDF serialization formats.


Lastly, if you run the same function over a parsed model, then the very slow
method above using SPARQL update is used.  This single-threaded manner of
ingesting appears to be particularly poor wrt performance.

```java
public void rdf4jLoadParsedTriples() throws IOException {
    MarkLogicRepository repository = new MarkLogicRepository(client());
    repository.initialize();
    MarkLogicRepositoryConnection conn = repository.getConnection();
    RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
    Model model = new LinkedHashModel();
    rdfParser.setRDFHandler(new StatementCollector(model));
    URL url = new URL("file:data/turtletriples/turtle600k.ttl");
    InputStream is = url.openStream();
    rdfParser.parse(is, "http://marklogic.com/bah/");
    conn.add(model);
}
```


After this change, these three methods of ingesting triples will have the same,
improved ingestion profile. See the implementation section for how we achieve
this goal.

TBD what we're not changing....


### triples no longer deduplicated on ingest

Currently, MarkLogics internal RDF parsing de-duplicates triples.  That is, if
a graph payload contains multiple copies of the same triple, the parsed
in-memory sequence of triples will only have one.

The newly implemented method does not deduplicate triples on ingest.



# Implementation


We target all uses of `MarkLogicRepositoryConnection.add()`

Each of the add() methods will adopt the same strategy for adding triples to MarkLogic.

1.  create a thread in which to execute the adds and return a Future.   block when?
1.  Implement a streaming triple parser using RDF4j that:
   1. starts a document at the beginning with <sem:triples> element.
   1. start counting triples.
   1. If we've processed 100 triples, end the document  with </sem:triples> and add it to a DMSDK batcher, start a new doc
   1.  Serialize each triple as a <sem:triple/> element.
1.  For quad parsing, refine the above process with a map.
   1. Each quad that arrives, create a key/value pair where the key is the graph name and the value is a stream of triples.
   1. As more quads arrive, add them
   1.  --- risk of hihh memory usage if lots of graphs.... TBD IN PROGRESS quads refinement DEAL WITH GRAPH DOCUMENTS.
1. in the Future, which must be handled at some point.. the transaction is rolled back or committed.


Note: The DMSDK batcher must be modified to support transaction.

# Open Issues/Risks

* graph documents. 

* permissions on graphs.  Probably not in play in this refactoring, as graph permissions are only accessible via an interface to MarkLogic specific things -- need to verify.

* marklogic-jena can benefit from the same approach but is not covered explicitly in this specification.

* Best impl will involve a change to the Java Client API (DMSDK) to include
transaction support in the write batcher.  This support probably should not be
generally used -- just by these clients -- because it's probably not a good
idea in most ingestion cases.  Alternately, multithreading and batching can be
done in each client library...



# Decisions

We considered the profile of server-side RDF parsing.  While there is room for
improvement in the server-side parsing, we determined that implementing this
client-side ingestion methodology will have more impact on the overall
ingestion landscape across the board.  This decision means that we have not
provided corresponding good ingestion methods in the other client language
libraries (node.js)


# Document History

2018-01-09 Created Document.
2018-01-10 Submitted to John James and Stephen Buxton for initial reaction.

