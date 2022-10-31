# Fiber ORM
#
# Copyright 2019 Jonathan Bernard <jonathan@jdbernard.com>

## Lightweight ORM supporting the `Postgres`_ and `SQLite`_ databases in Nim.
## It supports a simple, opinionated model mapper to generate SQL queries based
## on Nim objects. It also includes a simple connection pooling implementation.
##
## Fiber ORM is not intended to be a 100% all-cases-covered ORM that handles
## every potential data access pattern one might wish to implement. It is best
## thought of as a collection of common SQL generation patterns. It is intended
## to cover 90% of the common queries and functions one might write when
## implementing an SQL-based access layer. It is expected that there may be a
## few more complicated queries that need to be implemented to handle specific
## access patterns.
##
## The simple mapping pattern provided by Fiber ORM also works well on top of
## databases that encapsulate data access logic in SQL with, for example,
## views.
##
## .. _Postgres: https://nim-lang.org/docs/db_postgres.html
## .. _SQLite: https://nim-lang.org/docs/db_sqlite.html
##
## Basic Usage
## ===========
##
## Consider a simple TODO list application that keeps track of TODO items as
## well as time logged against those items.
##
## Example DB Schema
## -----------------
##
## You might have a schema such as:
##
## .. code-block:: SQL
##    create extension if not exists "pgcrypto";
##
##    create table todo_items columns (
##      id uuid not null primary key default gen_random_uuid(),
##      owner varchar not null,
##      summary varchar not null,
##      details varchar default null,
##      priority integer not null default 0,
##      related_todo_item_ids uuid[] not null default '{}'
##    );
##
##    create table time_entries columns (
##      id uuid not null primary key default gen_random_uuid(),
##      todo_item_id uuid not null references todo_items (id) on delete cascade,
##      start timestamp with timezone not null default current_timestamp,
##      stop timestamp with timezone default null,
##    );
##
## Example Model Definitions
## -------------------------
##
## Models may be defined as:
##
## .. code-block:: Nim
##    # models.nim
##    import std/options, std/times
##    import uuids
##
##    type
##      TodoItem* = object
##        id*: UUID
##        owner*: string
##        summary*: string
##        details*: Option[string]
##        priority*: int
##        relatedTodoItemIds*: seq[UUID]
##
##      TimeEntry* = object
##        id*: UUID
##        todoItemId*: Option[UUID]
##        start*: DateTime
##        stop*: Option[DateTime]
##
## Example Fiber ORM Usage
## -----------------------
##
## Using Fiber ORM we can generate a data access layer with:
##
## .. code-block:: Nim
##    # db.nim
##    import std/db_postgres
##    import fiber_orm
##    import ./models.nim
##
##    type TodoDB* = DbConnPool
##
##    proc initDb*(connString: string): TodoDB =
##      result = fiber_orm.initPool(
##        connect = proc(): DbConn = open("", "", "", connString),
##        poolSize = 20,
##        hardCap = false)
##
##
##    generateProcsForModels(TodoDB, [TodoItem, TimeEntry])
##
##    generateLookup(TodoDB, TimeEntry, @["todoItemId"])
##
## This will generate the following procedures:
##
## .. code-block:: Nim
##    proc getTodoItem*(db: TodoDB, id: UUID): TodoItem;
##
##    proc createTodoItem*(db: TodoDB, rec: TodoItem): TodoItem;
##    proc updateTodoItem*(db: TodoDB, rec: TodoItem): bool;
##    proc deleteTodoItem*(db: TodoDB, rec: TodoItem): bool;
##    proc deleteTodoItem*(db: TodoDB, id: UUID): bool;
##
##    proc getAllTodoItems*(db: TodoDB,
##      pagination = none[PaginationParams]()): seq[TodoItem];
##
##    proc findTodoItemsWhere*(db: TodoDB, whereClause: string,
##      values: varargs[string, dbFormat], pagination = none[PaginationParams]()
##      ): seq[TodoItem];
##
##    proc getTimeEntry*(db: TodoDB, id: UUID): TimeEntry;
##    proc createTimeEntry*(db: TodoDB, rec: TimeEntry): TimeEntry;
##    proc updateTimeEntry*(db: TodoDB, rec: TimeEntry): bool;
##    proc deleteTimeEntry*(db: TodoDB, rec: TimeEntry): bool;
##    proc deleteTimeEntry*(db: TodoDB, id: UUID): bool;
##
##    proc getAllTimeEntries*(db: TodoDB,
##      pagination = none[PaginationParams]()): seq[TimeEntry];
##
##    proc findTimeEntriesWhere*(db: TodoDB, whereClause: string,
##      values: varargs[string, dbFormat], pagination = none[PaginationParams]()
##      ): seq[TimeEntry];
##
##    proc findTimeEntriesByTodoItemId(db: TodoDB, todoItemId: UUID,
##      pagination = none[PaginationParams]()): seq[TimeEntry];
##
## Object-Relational Modeling
## ==========================
##
## Model Class
## -----------
##
## Fiber ORM uses simple Nim `object`s and `ref object`s as model classes.
## Fiber ORM expects there to be one table for each model class.
##
## Name Mapping
## ````````````
## Fiber ORM uses `snake_case` for database identifiers (column names, table
## names, etc.) and `camelCase` for Nim identifiers. We automatically convert
## model names to and from table names (`TodoItem` <-> `todo_items`), as well
## as column names (`userId` <-> `user_id`).
##
## Notice that table names are automatically pluralized from model class names.
## In the above example, you have:
##
## ===========    ================
## Model Class    Table Name
## ===========    ================
## TodoItem       todo_items
## TimeEntry      time_entries
## ===========    ================
##
## Because Nim is style-insensitive, you can generall refer to model classes
## and fields using `snake_case`, `camelCase`, or `PascalCase` in your code and
## expect Fiber ORM to be able to map the names to DB identifier names properly
## (though FiberORM will always use `camelCase` internally).
##
## See the `identNameToDb`_, `dbNameToIdent`_, `tableName`_ and `dbFormat`_
## procedures in the `fiber_orm/util`_ module for details.
##
## .. _identNameToDb: fiber_orm/util.html#identNameToDb,string
## .. _dbNameToIdent: fiber_orm/util.html#dbNameToIdent,string
## .. _tableName: fiber_orm/util.html#tableName,type
## .. _dbFormat: fiber_orm/util.html#dbFormat,DateTime
## .. _util: fiber_orm/util.html
##
## ID Field
## ````````
##
## Fiber ORM expects every model class to have a field named `id`, with a
## corresponding `id` column in the model table. This field must be either a
## `string`, `integer`, or `UUID`_.
##
## When creating a new record the `id` field will be omitted if it is empty
## (`Option.isNone`_, `UUID.isZero`_, value of `0`, or only whitespace).  This
## is intended to allow for cases like the example where the database may
## generate an ID when a new record is inserted. If a non-zero value is
## provided, the create call will include the `id` field in the `INSERT` query.
##
## For example, to allow the database to create the id:
##
## .. code-block:: Nim
##    let item = TodoItem(
##      owner: "John Mann",
##      summary: "Create a grocery list.",
##      details: none[string](),
##      priority: 0,
##      relatedTodoItemIds: @[])
##
##    let itemWithId = db.createTodoItem(item)
##    echo $itemWithId.id # generated in the database
##
## And to create it in code:
##
## .. code-block:: Nim
##    import uuids
##
##    let item = TodoItem(
##      id: genUUID(),
##      owner: "John Mann",
##      summary: "Create a grocery list.",
##      details: none[string](),
##      priority: 0,
##      relatedTodoItemIds: @[])
##
##    let itemInDb = db.createTodoItem(item)
##    echo $itemInDb.id # will be the same as what was provided
##
## .. _Option.isNone: https://nim-lang.org/docs/options.html#isNone,Option[T]
## .. _UUID.isZero: https://github.com/pragmagic/uuids/blob/8cb8720b567c6bcb261bd1c0f7491bdb5209ad06/uuids.nim#L72
##
## Supported Data Types
## --------------------
##
## The following Nim data types are supported by Fiber ORM:
##
## ===============  ======================  =================
## Nim Type         Postgres Type           SQLite Type
## ===============  ======================  =================
## `string`         `varchar`_
## `int`            `integer`_
## `float`          `double`_
## `bool`           `boolean`_
## `DateTime`_      `timestamp`_
## `seq[]`          `array`_
## `UUID`_          `uuid (pg)`_
## `Option`_        *allows* `NULL` [#f1]_
## `JsonNode`_      `jsonb`_
## ===============  ======================  =================
##
## .. [#f1] Note that this implies that all `NULL`-able fields should be typed
##          as optional using `Option[fieldType]`. Conversely, any fields with
##          non-optional types should also be constrained to be `NOT NULL` in
##          the database schema.
##
## .. _DateTime: https://nim-lang.org/docs/times.html#DateTime
## .. _UUID: https://github.com/pragmagic/uuids
## .. _Option: https://nim-lang.org/docs/options.html#Option
## .. _JsonNode: https://nim-lang.org/docs/json.html#JsonNode
##
## .. _varchar: https://www.postgresql.org/docs/current/datatype-character.html
## .. _integer: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-INT
## .. _double: https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-FLOAT
## .. _boolean: https://www.postgresql.org/docs/current/datatype-boolean.html
## .. _timestamp: https://www.postgresql.org/docs/current/datatype-datetime.html
## .. _array: https://www.postgresql.org/docs/current/arrays.html
## .. _uuid (pg): https://www.postgresql.org/docs/current/datatype-uuid.html
## .. _jsonb: https://www.postgresql.org/docs/current/datatype-json.html
##
## Database Object
## ===============
##
## Many of the Fiber ORM macros expect a database object type to be passed.
## In the example above the `pool.DbConnPool`_ object is used as database
## object type (aliased as `TodoDB`). This is the intended usage pattern, but
## anything can be passed as the database object type so long as there is a
## defined `withConn` template that provides an injected `conn: DbConn` object
## to the provided statement body.
##
## For example, a valid database object implementation that opens a new
## connection for every request might look like this:
##
## .. code-block:: Nim
##    import std/db_postgres
##
##    type TodoDB* = object
##      connString: string
##
##    template withConn*(db: TodoDB, stmt: untyped): untyped =
##      let conn {.inject.} = open("", "", "", db.connString)
##      try: stmt
##      finally: close(conn)
##
## .. _pool.DbConnPool: fiber_orm/pool.html#DbConnPool
##
import std/[db_common, logging, macros, options, sequtils, strutils]
import namespaced_logging, uuids

from std/unicode import capitalize

import ./fiber_orm/db_common as fiber_db_common
import ./fiber_orm/pool
import ./fiber_orm/util

export
  pool,
  util.columnNamesForModel,
  util.dbFormat,
  util.dbNameToIdent,
  util.identNameToDb,
  util.modelName,
  util.rowToModel,
  util.tableName

type
  PaginationParams* = object
    pageSize*: int
    offset*: int
    orderBy*: Option[string]

  PagedRecords*[T] = object
    pagination*: Option[PaginationParams]
    records*: seq[T]
    totalRecords*: int

  NotFoundError* = object of CatchableError ##\
    ## Error type raised when no record matches a given ID

var logNs {.threadvar.}: LoggingNamespace

template log(): untyped =
  if logNs.isNil: logNs = getLoggerForNamespace(namespace = "fiber_orm", level = lvlNotice)
  logNs

proc newMutateClauses(): MutateClauses =
  return MutateClauses(
    columns: @[],
    placeholders: @[],
    values: @[])

proc createRecord*[D: DbConnType, T](db: D, rec: T): T =
  ## Create a new record. `rec` is expected to be a `model class`_. The `id`
  ## field is only set if it is non-empty (see `ID Field`_ for details).
  ##
  ## Returns the newly created record.
  ##
  ## .. _model class: #objectminusrelational-modeling-model-class
  ## .. _ID Field: #model-class-id-field

  var mc = newMutateClauses()
  populateMutateClauses(rec, true, mc)

  let sqlStmt =
    "INSERT INTO " & tableName(rec) &
    " (" & mc.columns.join(",") & ") " &
    " VALUES (" & mc.placeholders.join(",") & ") " &
    " RETURNING " & columnNamesForModel(rec).join(",")

  log().debug "createRecord: [" & sqlStmt & "]"
  let newRow = db.getRow(sql(sqlStmt), mc.values)

  result = rowToModel(T, newRow)

proc updateRecord*[D: DbConnType, T](db: D, rec: T): bool =
  ## Update a record by id. `rec` is expected to be a `model class`_.
  var mc = newMutateClauses()
  populateMutateClauses(rec, false, mc)

  let setClause = zip(mc.columns, mc.placeholders).mapIt(it[0] & " = " & it[1]).join(",")
  let sqlStmt =
    "UPDATE " & tableName(rec) &
    " SET " & setClause &
    " WHERE id = ? "

  log().debug "updateRecord: [" & sqlStmt & "] id: " & $rec.id
  let numRowsUpdated = db.execAffectedRows(sql(sqlStmt), mc.values.concat(@[$rec.id]))

  return numRowsUpdated > 0;

template deleteRecord*[D: DbConnType](db: D, modelType: type, id: typed): untyped =
  ## Delete a record by id.
  let sqlStmt = "DELETE FROM " & tableName(modelType) & " WHERE id = ?"
  log().debug "deleteRecord: [" & sqlStmt & "] id: " & $id
  db.tryExec(sql(sqlStmt), $id)

proc deleteRecord*[D: DbConnType, T](db: D, rec: T): bool =
  ## Delete a record by `id`_.
  ##
  ## .. _id: #model-class-id-field
  let sqlStmt = "DELETE FROM " & tableName(rec) & " WHERE id = ?"
  log().debug "deleteRecord: [" & sqlStmt & "] id: " & $rec.id
  return db.tryExec(sql(sqlStmt), $rec.id)

template getRecord*[D: DbConnType](db: D, modelType: type, id: typed): untyped =
  ## Fetch a record by id.
  let sqlStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType) &
    " WHERE id = ?"

  log().debug "getRecord: [" & sqlStmt & "] id: " & $id
  let row = db.getRow(sql(sqlStmt), @[$id])

  if allIt(row, it.len == 0):
    raise newException(NotFoundError, "no " & modelName(modelType) & " record for id " & $id)

  rowToModel(modelType, row)

template findRecordsWhere*[D: DbConnType](
    db: D,
    modelType: type,
    whereClause: string,
    values: varargs[string, dbFormat],
    page: Option[PaginationParams]): untyped =
  ## Find all records matching a given `WHERE` clause. The number of elements in
  ## the `values` array must match the number of placeholders (`?`) in the
  ## provided `WHERE` clause.
  var fetchStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType) &
    " WHERE " & whereClause

  var countStmt =
    "SELECT COUNT(*) FROM " & tableName(modelType) &
    " WHERE " & whereClause

  if page.isSome:
    let p = page.get
    if p.orderBy.isSome:
      fetchStmt &= " ORDER BY " & p.orderBy.get
    else:
      fetchStmt &= " ORDER BY id"

    fetchStmt &= " LIMIT " & $p.pageSize &
                 " OFFSET " & $p.offset

  log().debug "findRecordsWhere: [" & fetchStmt & "] values: (" & values.join(", ") & ")"
  let records = db.getAllRows(sql(fetchStmt), values).mapIt(rowToModel(modelType, it))

  PagedRecords[modelType](
    pagination: page,
    records: records,
    totalRecords:
      if page.isNone: records.len
      else: db.getRow(sql(countStmt), values)[0].parseInt)

template getAllRecords*[D: DbConnType](
    db: D,
    modelType: type,
    page: Option[PaginationParams]): untyped =
  ## Fetch all records of the given type.
  var fetchStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType)

  var countStmt = "SELECT COUNT(*) FROM " & tableName(modelType)

  if page.isSome:
    let p = page.get
    if p.orderBy.isSome:
      fetchStmt &= " ORDER BY " & p.orderBy.get
    else:
      fetchStmt &= " ORDER BY id"

    fetchStmt &= " LIMIT " & $p.pageSize &
                 " OFFSET " & $p.offset

  log().debug "getAllRecords: [" & fetchStmt & "]"
  let records = db.getAllRows(sql(fetchStmt)).mapIt(rowToModel(modelType, it))

  PagedRecords[modelType](
    pagination: page,
    records: records,
    totalRecords:
      if page.isNone: records.len
      else: db.getRow(sql(countStmt))[0].parseInt)


template findRecordsBy*[D: DbConnType](
    db: D,
    modelType: type,
    lookups: seq[tuple[field: string, value: string]],
    page: Option[PaginationParams]): untyped =
  ## Find all records matching the provided lookup values.
  let whereClause = lookups.mapIt(it.field & " = ?").join(" AND ")
  let values = lookups.mapIt(it.value)

  var fetchStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType) &
    " WHERE " & whereClause

  var countStmt =
    "SELECT COUNT(*) FROM " & tableName(modelType) &
    " WHERE " & whereClause

  if page.isSome:
    let p = page.get
    if p.orderBy.isSome:
      fetchStmt &= " ORDER BY " & p.orderBy.get
    else:
      fetchStmt &= " ORDER BY id"

    fetchStmt &= " LIMIT " & $p.pageSize &
                 " OFFSET " & $p.offset

  log().debug "findRecordsBy: [" & fetchStmt & "] values (" & values.join(", ") & ")"
  let records = db.getAllRows(sql(fetchStmt), values).mapIt(rowToModel(modelType, it))

  PagedRecords[modelType](
    pagination: page,
    records: records,
    totalRecords:
      if page.isNone: records.len
      else: db.getRow(sql(countStmt), values)[0].parseInt)


macro generateProcsForModels*(dbType: type, modelTypes: openarray[type]): untyped =
  ## Generate all standard access procedures for the given model types. For a
  ## `model class`_ named `TodoItem`, this will generate the following
  ## procedures:
  ##
  ## .. code-block:: Nim
  ##    proc getTodoItem*(db: TodoDB, id: idType): TodoItem;
  ##    proc getAllTodoItems*(db: TodoDB): TodoItem;
  ##    proc createTodoItem*(db: TodoDB, rec: TodoItem): TodoItem;
  ##    proc deleteTodoItem*(db: TodoDB, rec: TodoItem): bool;
  ##    proc deleteTodoItem*(db: TodoDB, id: idType): bool;
  ##    proc updateTodoItem*(db: TodoDB, rec: TodoItem): bool;
  ##
  ##    proc findTodoItemsWhere*(
  ##      db: TodoDB, whereClause: string, values: varargs[string]): TodoItem;
  ##
  ## `dbType` is expected to be some type that has a defined `withConn`
  ## procedure (see `Database Object`_ for details).
  ##
  ## .. _Database Object: #database-object
  result = newStmtList()

  for t in modelTypes:
    if t.getType[1].typeKind == ntyRef:
      raise newException(ValueError,
        "fiber_orm model object must be objects, not refs")

    let modelName = $(t.getType[1])
    let getName = ident("get" & modelName)
    let getAllName = ident("getAll" & pluralize(modelName))
    let findWhereName = ident("find" & pluralize(modelName) & "Where")
    let createName = ident("create" & modelName)
    let updateName = ident("update" & modelName)
    let deleteName = ident("delete" & modelName)
    let idType = typeOfColumn(t, "id")
    result.add quote do:
      proc `getName`*(db: `dbType`, id: `idType`): `t` =
        db.withConn: result = getRecord(conn, `t`, id)

      proc `getAllName`*(db: `dbType`, pagination = none[PaginationParams]()): PagedRecords[`t`] =
        db.withConn: result = getAllRecords(conn, `t`, pagination)

      proc `findWhereName`*(
          db: `dbType`,
          whereClause: string,
          values: varargs[string, dbFormat],
          pagination = none[PaginationParams]()): PagedRecords[`t`] =
        db.withConn:
          result = findRecordsWhere(conn, `t`, whereClause, values, pagination)

      proc `createName`*(db: `dbType`, rec: `t`): `t` =
        db.withConn: result = createRecord(conn, rec)

      proc `updateName`*(db: `dbType`, rec: `t`): bool =
        db.withConn: result = updateRecord(conn, rec)

      proc `deleteName`*(db: `dbType`, rec: `t`): bool =
        db.withConn: result = deleteRecord(conn, rec)

      proc `deleteName`*(db: `dbType`, id: `idType`): bool =
        db.withConn: result = deleteRecord(conn, `t`, id)

macro generateLookup*(dbType: type, modelType: type, fields: seq[string]): untyped =
  ## Create a lookup procedure for a given set of field names. For example,
  ## given the TODO database demostrated above,
  ##
  ## .. code-block:: Nim
  ##    generateLookup(TodoDB, TodoItem, ["owner", "priority"])
  ##
  ## will generate the following procedure:
  ##
  ## .. code-block:: Nim
  ##    proc findTodoItemsByOwnerAndPriority*(db: SampleDB,
  ##      owner: string, priority: int): seq[TodoItem]
  let fieldNames = fields[1].mapIt($it)
  let procName = ident("find" & pluralize($modelType.getType[1]) & "By" & fieldNames.mapIt(it.capitalize).join("And"))

  # Create proc skeleton
  result = quote do:
    proc `procName`*(db: `dbType`): PagedRecords[`modelType`] =
      db.withConn: result = findRecordsBy(conn, `modelType`)

  var callParams = quote do: @[]

  # Add dynamic parameters for the proc definition and inner proc call
  for n in fieldNames:
    let paramTuple = newNimNode(nnkPar)
    paramTuple.add(newColonExpr(ident("field"), newLit(identNameToDb(n))))
    paramTuple.add(newColonExpr(ident("value"), ident(n)))

    # Add the parameter to the outer call (the generated proc)
    # result[3] is ProcDef -> [3]: FormalParams
    result[3].add(newIdentDefs(ident(n), ident("string")))

    # Build up the AST for the inner procedure call
    callParams[1].add(paramTuple)

  # Add the optional pagination parameters to the generated proc definition
  result[3].add(newIdentDefs(
    ident("pagination"), newEmptyNode(),
    quote do: none[PaginationParams]()))

  # Add the call params to the inner procedure call
  # result[6][0][1][0][1] is
  #   ProcDef -> [6]: StmtList (body) -> [0]: Call ->
  #     [1]: StmtList (withConn body) -> [0]: Asgn (result =) ->
  #     [1]: Call (inner findRecords invocation)
  result[6][0][1][0][1].add(callParams)
  result[6][0][1][0][1].add(quote do: pagination)

macro generateProcsForFieldLookups*(dbType: type, modelsAndFields: openarray[tuple[t: type, fields: seq[string]]]): untyped =
  result = newStmtList()

  for i in modelsAndFields:
    var modelType = i[1][0]
    let fieldNames = i[1][1][1].mapIt($it)

    let procName = ident("find" & $modelType & "sBy" & fieldNames.mapIt(it.capitalize).join("And"))

    # Create proc skeleton
    let procDefAST = quote do:
      proc `procName`*(db: `dbType`): PagedRecords[`modelType`] =
        db.withConn: result = findRecordsBy(conn, `modelType`)

    var callParams = quote do: @[]

    # Add dynamic parameters for the proc definition and inner proc call
    for n in fieldNames:
      let paramTuple = newNimNode(nnkPar)
      paramTuple.add(newColonExpr(ident("field"), newLit(identNameToDb(n))))
      paramTuple.add(newColonExpr(ident("value"), ident(n)))

      procDefAST[3].add(newIdentDefs(ident(n), ident("string")))
      callParams[1].add(paramTuple)

    # Add the optional pagination parameters to the generated proc definition
    procDefAST[3].add(newIdentDefs(
      ident("pagination"), newEmptyNode(),
      quote do: none[PaginationParams]()))

    procDefAST[6][0][1][0][1].add(callParams)
    procDefAST[6][0][1][0][1].add(quote do: pagination)

    result.add procDefAST

proc initPool*[D: DbConnType](
    connect: proc(): D,
    poolSize = 10,
    hardCap = false,
    healthCheckQuery = "SELECT 'true' AS alive"): DbConnPool[D] =

  ## Initialize a new DbConnPool. See the `initDb` procedure in the `Example
  ## Fiber ORM Usage`_ for an example
  ##
  ## * `connect` must be a factory which creates a new `DbConn`.
  ## * `poolSize` sets the desired capacity of the connection pool.
  ## * `hardCap` defaults to `false`.
  ##   When `false`, the pool can grow beyond the configured capacity, but will
  ##   release connections down to the its capacity (no less than `poolSize`).
  ##
  ##   When `true` the pool will not create more than its configured capacity.
  ##   It a connection is requested, none are free, and the pool is at
  ##   capacity, this will result in an Error being raised.
  ## * `healthCheckQuery` should be a simple and fast SQL query that the pool
  ##   can use to test the liveliness of pooled connections.
  ##
  ## .. _Example Fiber ORM Usage: #basic-usage-example-fiber-orm-usage

  initDbConnPool(DbConnPoolConfig[D](
    connect: connect,
    poolSize: poolSize,
    hardCap: hardCap,
    healthCheckQuery: healthCheckQuery))

template inTransaction*[D: DbConnType](db: DbConnPool[D], body: untyped) =
  pool.withConn(db):
    conn.exec(sql"BEGIN TRANSACTION")
    try:
      body
      conn.exec(sql"COMMIT")
    except:
      conn.exec(sql"ROLLBACK")
      raise getCurrentException()
