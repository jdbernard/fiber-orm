Fiber ORM
~~~~~~~~~

Lightweight ORM supporting the `Postgres`_ and `SQLite`_ databases in Nim.
It supports a simple, opinionated model mapper to generate SQL queries based
on Nim objects. It also includes a simple connection pooling implementation.

Fiber ORM is not intended to be a 100% all-cases-covered ORM that handles
every potential data access pattern one might wish to implement. It is best
thought of as a collection of common SQL generation patterns. It is intended
to cover 90% of the common queries and functions one might write when
implementing an SQL-based access layer. It is expected that there may be a
few more complicated queries that need to be implemented to handle specific
access patterns.

The simple mapping pattern provided by Fiber ORM also works well on top of
databases that encapsulate data access logic in SQL with, for example,
views.

.. _Postgres: https://nim-lang.org/docs/db_postgres.html
.. _SQLite: https://nim-lang.org/docs/db_sqlite.html

Basic Usage
===========

Consider a simple TODO list application that keeps track of TODO items as
well as time logged against those items.

Example DB Schema
-----------------

You might have a schema such as:

.. code-block:: SQL
   create extension if not exists "pgcrypto";

   create table todo_items columns (
     id uuid not null primary key default gen_random_uuid(),
     owner varchar not null,
     summary varchar not null,
     details varchar default null,
     priority integer not null default 0,
     related_todo_item_ids uuid[] not null default '{}'
   );

   create table time_entries columns (
     id uuid not null primary key default gen_random_uuid(),
     todo_item_id uuid not null references todo_items (id) on delete cascade,
     start timestamp with timezone not null default current_timestamp,
     stop timestamp with timezone default null,
   );

Example Model Definitions
-------------------------

Models may be defined as:

.. code-block:: Nim
   # models.nim
   import std/options, std/times
   import uuids

   type
     TodoItem* = object
       id*: UUID
       owner*: string
       summary*: string
       details*: Option[string]
       priority*: int
       relatedTodoItemIds*: seq[UUID]

     TimeEntry* = object
       id*: UUID
       todoItemId*: Option[UUID]
       start*: DateTime
       stop*: Option[DateTime]

Example Fiber ORM Usage
-----------------------

Using Fiber ORM we can generate a data access layer with:

.. code-block:: Nim
   # db.nim
   import fiber_orm
   import ./models.nim

   type TodoDB* = DbConnPool

   proc initDb*(connString: string): TodoDB =
     result = fiber_orm.initPool(
       connect = proc(): DbConn = open("", "", "", connString),
       poolSize = 20,
       hardCap = false)


   generateProcsForModels(TodoDB, [TodoItem, TimeEntry])

   generateLookup(TodoDB, TimeEntry, @["todoItemId"])

This will generate the following procedures:

.. code-block:: Nim
   proc getTodoItem*(db: TodoDB, id: UUID): TodoItem;
   proc getAllTodoItems*(db: TodoDB): seq[TodoItem];
   proc createTodoItem*(db: TodoDB, rec: TodoItem): TodoItem;
   proc updateTodoItem*(db: TodoDB, rec: TodoItem): bool;
   proc deleteTodoItem*(db: TodoDB, rec: TodoItem): bool;
   proc deleteTodoItem*(db: TodoDB, id: UUID): bool;

   proc findTodoItemsWhere*(db: TodoDB, whereClause: string,
     values: varargs[string, dbFormat]): seq[TodoItem];

   proc getTimeEntry*(db: TodoDB, id: UUID): TimeEntry;
   proc getAllTimeEntries*(db: TodoDB): seq[TimeEntry];
   proc createTimeEntry*(db: TodoDB, rec: TimeEntry): TimeEntry;
   proc updateTimeEntry*(db: TodoDB, rec: TimeEntry): bool;
   proc deleteTimeEntry*(db: TodoDB, rec: TimeEntry): bool;
   proc deleteTimeEntry*(db: TodoDB, id: UUID): bool;

   proc findTimeEntriesWhere*(db: TodoDB, whereClause: string,
     values: varargs[string, dbFormat]): seq[TimeEntry];

   proc findTimeEntriesByTodoItemId(db: TodoDB, todoItemId: UUID): seq[TimeEntry];

Object-Relational Modeling
==========================

Model Class
-----------

Fiber ORM uses simple Nim `object`s and `ref object`s as model classes.
Fiber ORM expects there to be one table for each model class.

Name Mapping
````````````
Fiber ORM uses `snake_case` for database identifiers (column names, table
names, etc.) and `camelCase` for Nim identifiers. We automatically convert
model names to and from table names (`TodoItem` <-> `todo_items`), as well
as column names (`userId` <-> `user_id`).

Notice that table names are automatically pluralized from model class names.
In the above example, you have:

===========    ================
Model Class    Table Name
===========    ================
TodoItem       todo_items
TimeEntry      time_entries
===========    ================

Because Nim is style-insensitive, you can generall refer to model classes
and fields using `snake_case`, `camelCase`, or `PascalCase` in your code and
expect Fiber ORM to be able to map the names to DB identifier names properly
(though FiberORM will always use `camelCase` internally).

See the `identNameToDb`_, `dbNameToIdent`_, `tableName`_ and `dbFormat`_
procedures in the `fiber_orm/util`_ module for details.

.. _identNameToDb: fiber_orm/util.html#identNameToDb,string
.. _dbNameToIdent: fiber_orm/util.html#dbNameToIdent,string
.. _tableName: fiber_orm/util.html#tableName,type
.. _dbFormat: fiber_orm/util.html#dbFormat,DateTime
.. _util: fiber_orm/util.html

ID Field
````````

Fiber ORM expects every model class to have a field named `id`, with a
corresponding `id` column in the model table. This field must be either a
`string`, `integer`, or `UUID`_.

When creating a new record the `id` field will be omitted if it is empty
(`Option.isNone`_, `UUID.isZero`_, value of `0`, or only whitespace).  This
is intended to allow for cases like the example where the database may
generate an ID when a new record is inserted. If a non-zero value is
provided, the create call will include the `id` field in the `INSERT` query.

For example, to allow the database to create the id:

.. code-block:: Nim
   let item = TodoItem(
     owner: "John Mann",
     summary: "Create a grocery list.",
     details: none[string](),
     priority: 0,
     relatedTodoItemIds: @[])

   let itemWithId = db.createTodoItem(item)
   echo $itemWithId.id # generated in the database

And to create it in code:

.. code-block:: Nim
   import uuids

   let item = TodoItem(
     id: genUUID(),
     owner: "John Mann",
     summary: "Create a grocery list.",
     details: none[string](),
     priority: 0,
     relatedTodoItemIds: @[])

   let itemInDb = db.createTodoItem(item)
   echo $itemInDb.id # will be the same as what was provided

.. _Option.isNone: https://nim-lang.org/docs/options.html#isNone,Option[T]
.. _UUID.isZero: https://github.com/pragmagic/uuids/blob/8cb8720b567c6bcb261bd1c0f7491bdb5209ad06/uuids.nim#L72

Supported Data Types
--------------------

The following Nim data types are supported by Fiber ORM:

===============  ======================  =================
Nim Type         Postgres Type           SQLite Type
===============  ======================  =================
`string`         `varchar`_
`int`            `integer`_
`float`          `double`_
`bool`           `boolean`_
`DateTime`_      `timestamp`_
`seq[]`          `array`_
`UUID`_          `uuid (pg)`_
`Option`_        *allows* `NULL` [#f1]_
`JsonNode`_      `jsonb`_
===============  ======================  =================

.. [#f1] Note that this implies that all `NULL`-able fields should be typed
         as optional using `Option[fieldType]`. Conversely, any fields with
         non-optional types should also be constrained to be `NOT NULL` in
         the database schema.

.. _DateTime: https://nim-lang.org/docs/times.html#DateTime
.. _UUID: https://github.com/pragmagic/uuids
.. _Option: https://nim-lang.org/docs/options.html#Option
.. _JsonNode: https://nim-lang.org/docs/json.html#JsonNode

.. _varchar: https://www.postgresql.org/docs/current/datatype-character.html
.. _integer: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
.. _double: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT
.. _boolean: https://www.postgresql.org/docs/current/datatype-boolean.html
.. _timestamp: https://www.postgresql.org/docs/current/datatype-datetime.html
.. _array: https://www.postgresql.org/docs/current/arrays.html
.. _uuid (pg): https://www.postgresql.org/docs/current/datatype-uuid.html
.. _jsonb: https://www.postgresql.org/docs/current/datatype-json.html

Database Object
===============

Many of the Fiber ORM macros expect a database object type to be passed.
In the example above the `pool.DbConnPool`_ object is used as database
object type (aliased as `TodoDB`). This is the intended usage pattern, but
anything can be passed as the database object type so long as there is a
defined `withConn` template that provides an injected `conn: DbConn` object
to the provided statement body.

For example, a valid database object implementation that opens a new
connection for every request might look like this:

.. code-block:: Nim
   import std/db_postgres

   type TodoDB* = object
     connString: string

   template withConn*(db: TodoDB, stmt: untyped): untyped =
     let conn {.inject.} = open("", "", "", db.connString)
     try: stmt
     finally: close(conn)

.. _pool.DbConnPool: fiber_orm/pool.html#DbConnPool
