Generic solution for connecting to any DB via unixODBC from PostgreSQL
======================================================================

Compilation and Installation
============================

Requirements are:
- a recent unixODBC version under UNIX/Linux
- ODBC driver for the required DBMS

Compile the module:

$ USE_PGXS=1 make

Install as root:

# USE_PGXS=1 make install

Setup
=====

Load the odbclink.sql to the appropriate database:

$ psql -f odbclink.sql dbname

The module requires an ODBC DSN correctly set up, e.g. for Informix
download and install the Informix Client-SDK from IBM and use this
information to set up the ODBC DataSource:
http://www.unixodbc.org/doc/informix.html

Usage
=====

First, the connection must be performed:

dbname=# select odbclink.connect('dsn', 'username', 'password');

 connect 
---------
       1
(1 row)

or:

dbname=# select odbclink.connect('DSN=dsn;UID=username;PWD=password;');

 connect 
---------
       2
(1 row)

The function returns the # of the connection. Multiple connections
can be active at a time, they don't have to use the same remote server.

Connections can be queried:

dbname=# select * from odbclink.connections();
 id | connected | dsn |   uid    |   pwd    |              connstr 
----+-----------+-----+----------+----------+------------------------------------
  1 | t         | dsn | username | password | 
  2 | t         |     |          |          | DSN=dsn;UID=username;PWD=password;
  3 | f         |     |          |          | 
  4 | f         |     |          |          | 
(4 rows)

Disconnection can be performed explicitely:

dbname=# select odbclink.disconnect(1);
 disconnect 
------------
 
(1 row)

It's not mandatory to disconnect manually, it's automatic upon
disconnecting from the PostgreSQL server.

Queries that require no parameters can be performed using either
the connection number:

dbname=# select * from odbclink.query(1, 'SELECT * FROM mytable') as result(id int4, t text, d decimal);
 id |      t       |  d  
----+--------------+-----
  1 | first text   | 1.0
  2 | second row   | 1.5
  3 | third client |  20
(3 rows)

or the DSN/UID/PWD triplet:

dbname=# select * from odbclink.query('dsn', 'username', 'password', 'SELECT * FROM mytable') as result(id int4, t text, d decimal);
 id |      t       |  d  
----+--------------+-----
  1 | first text   | 1.0
  2 | second row   | 1.5
  3 | third client |  20
(3 rows)

or the connection string:

dbname=# select * from odbclink.query('DSN=dsn;UID=username;PWD=password;', 'SELECT * FROM mytable') as result(id int4, t text, d decimal);
 id |      t       |  d  
----+--------------+-----
  1 | first text   | 1.0
  2 | second row   | 1.5
  3 | third client |  20
(3 rows)

In the last two cases, the DSN/UID/PWD triplets and the connection
strings are cached in the internal tables, so no re-connection
occurs if the same DSN/UID/PWD triplet or connection string is
used for different queries, instead it uses the already open connection.

All three forms of the function odbclink.query() returns "SETOF RECORD", so
1. it can return different query results
2. it must be properly casted to the expected result structure

Types defined by ODBC are supported. The SQL_*BINARY types are only
supported from PostgreSQL 8.5/9.0+.

Executing DML statements are also supported using odbclink.execute() calls.
Like odbclink.query(), 3 variants of this function exist. These function
returns VOID. For example, let's create a new table in the remote database,
query this table, add two records to it then query it again:

dbname=# select odbclink.execute(1, 'create table test_table(i serial, t text)');
 execute 
---------
 
(1 row)

dbname=# select * from odbclink.query(1, 'select * from test_table') as x(i int4, t text);
 i | t 
---+---
(0 rows)

dbname=# select odbclink.execute(1, 'insert into test_table(t) values (''a''), (''b'')');
 execute 
---------
 
(1 row)

dbname=# select * from odbclink.query(1, 'select * from test_table') as x(i int4, t text);
 i | t 
---+---
 1 | a
 2 | b
(2 rows)

(C) 2010-2012. Cybertec GmbH
Zoltán Böszörményi <zb@cybertec.at>
Hans-Jürgen Schönig <hs@cybertec.at>
