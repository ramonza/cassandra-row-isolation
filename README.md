Cassandra row-level isolation test
===

We create a table (key, x, y) such that for each distinct key, the invariant x + y = 10 is supposed to hold.
Since for a given key (the CQL primary key), x and y are columns in the same row we should be able to maintain
this invariant even while updating the values of x and y - this is row isolation that Cassandra is supposed to support
since 1.1.

Unfortunately, this does not work even on a single node cluster. Tested on Cassandra 2.0.1.

To run the test you need Cassandra running on localhost, then run:

    ./gradlew run
