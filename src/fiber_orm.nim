# Fiber ORM
#
# Copyright 2019 Jonathan Bernard <jonathan@jdbernard.com>

## Lightweight ORM supporting the `Postgres`_ and `SQLite`_ databases in Nim.
## It supports a simple, opinionated model mapper to generate SQL queries based
## on Nim objects. It also includes a simple connection pooling implementation.
##
## .. _Postgres: https://nim-lang.org/docs/db_postgres.html
## .. _SQLite: https://nim-lang.org/docs/db_sqlite.html
##
## Basic Usage
## ===========
##
## Object-Relational Modeling
## ==========================
##
## Model Class
## -----------
##
## Table Name
## ``````````
##
## Column Names
## ````````````
##
## ID Field
## ````````
##
## Supported Data Types
## --------------------
##
## Database Object
## ===============

import std/db_postgres, std/macros, std/options, std/sequtils, std/strutils
import namespaced_logging, uuids

from std/unicode import capitalize

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

type NotFoundError* = object of CatchableError ##\
  ## Error type raised when no record matches a given ID

var logNs {.threadvar.}: LoggingNamespace

template log(): untyped =
  if logNs.isNil: logNs = initLoggingNamespace(name = "fiber_orm", level = lvlNotice)
  logNs

proc newMutateClauses(): MutateClauses =
  return MutateClauses(
    columns: @[],
    placeholders: @[],
    values: @[])

proc createRecord*[T](db: DbConn, rec: T): T =
  ## Create a new record. `rec` is expected to be a `model class`_. The `id
  ## field`_ is only set if it is `non-empty`_
  ##
  ## Returns the newly created record.
  ##
  ## .. _model class: #objectminusrelational-modeling-model-class
  ## .. _id field: #model-class-id-field
  ## .. _non-empty:

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

proc updateRecord*[T](db: DbConn, rec: T): bool =
  ## Update a record by id. `rec` is expected to be a `model class`_.
  ##
  ## .. _model class: #objectminusrelational-modeling-model-class
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

template deleteRecord*(db: DbConn, modelType: type, id: typed): untyped =
  ## Delete a record by id.
  let sqlStmt = "DELETE FROM " & tableName(modelType) & " WHERE id = ?"
  log().debug "deleteRecord: [" & sqlStmt & "] id: " & $id
  db.tryExec(sql(sqlStmt), $id)

proc deleteRecord*[T](db: DbConn, rec: T): bool =
  ## Delete a record by `id`_.
  ##
  ## .. _id: #model-class-id-field
  let sqlStmt = "DELETE FROM " & tableName(rec) & " WHERE id = ?"
  log().debug "deleteRecord: [" & sqlStmt & "] id: " & $rec.id
  return db.tryExec(sql(sqlStmt), $rec.id)

template getRecord*(db: DbConn, modelType: type, id: typed): untyped =
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

template findRecordsWhere*(db: DbConn, modelType: type, whereClause: string, values: varargs[string, dbFormat]): untyped =
  ## Find all records matching a given `WHERE` clause. The number of elements in
  ## the `values` array must match the number of placeholders (`?`) in the
  ## provided `WHERE` clause.
  let sqlStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType) &
    " WHERE " & whereClause

  log().debug "findRecordsWhere: [" & sqlStmt & "] values: (" & values.join(", ") & ")"
  db.getAllRows(sql(sqlStmt), values).mapIt(rowToModel(modelType, it))

template getAllRecords*(db: DbConn, modelType: type): untyped =
  ## Fetch all records of the given type.
  let sqlStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType)

  log().debug "getAllRecords: [" & sqlStmt & "]"
  db.getAllRows(sql(sqlStmt)).mapIt(rowToModel(modelType, it))

template findRecordsBy*(db: DbConn, modelType: type, lookups: seq[tuple[field: string, value: string]]): untyped =
  ## Find all records matching the provided lookup values.
  let sqlStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType) &
    " WHERE " & lookups.mapIt(it.field & " = ?").join(" AND ")
  let values = lookups.mapIt(it.value)

  log().debug "findRecordsBy: [" & sqlStmt & "] values (" & values.join(", ") & ")"
  db.getAllRows(sql(sqlStmt), values).mapIt(rowToModel(modelType, it))

macro generateProcsForModels*(dbType: type, modelTypes: openarray[type]): untyped =
  ## Generate all standard access procedures for the given model types. For a
  ## `model class`_ named `SampleRecord`, this will generate the following
  ## procedures:
  ##
  ## .. code-block:: Nim
  ##    proc getSampleRecord*(db: dbType): SampleRecord;
  ##    proc getAllSampleRecords*(db: dbType): SampleRecord;
  ##    proc createSampleRecord*(db: dbType, rec: SampleRecord): SampleRecord;
  ##    proc deleteSampleRecord*(db: dbType, rec: SampleRecord): bool;
  ##    proc deleteSampleRecord*(db: dbType, id: idType): bool;
  ##    proc updateSampleRecord*(db: dbType, rec: SampleRecord): bool;
  ##
  ##    proc findSampleRecordsWhere*(
  ##      db: dbType, whereClause: string, values: varargs[string]): SampleRecord;
  ##
  ## `dbType` is expected to be some type that has a defined `withConn`_
  ## procedure.
  result = newStmtList()

  for t in modelTypes:
    let modelName = $(t.getType[1])
    let getName = ident("get" & modelName)
    let getAllName = ident("getAll" & modelName & "s")
    let findWhereName = ident("find" & modelName & "sWhere")
    let createName = ident("create" & modelName)
    let updateName = ident("update" & modelName)
    let deleteName = ident("delete" & modelName)
    let idType = typeOfColumn(t, "id")
    result.add quote do:
      proc `getName`*(db: `dbType`, id: `idType`): `t` =
        db.withConn: result = getRecord(conn, `t`, id)

      proc `getAllName`*(db: `dbType`): seq[`t`] =
        db.withConn: result = getAllRecords(conn, `t`)

      proc `findWhereName`*(db: `dbType`, whereClause: string, values: varargs[string, dbFormat]): seq[`t`] =
        db.withConn:
          result = findRecordsWhere(conn, `t`, whereClause, values)

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
  ##
  ## .. code-block:: Nim
  ##    generateLookup(SampleDB, SampleRecord, ["name", "location"])
  ##
  ## will generate the following procedure:
  ##
  ## .. code-block:: Nim
  ##    proc findSampleRecordsByNameAndLocation*(db: SampleDB,
  let fieldNames = fields[1].mapIt($it)
  let procName = ident("find" & pluralize($modelType.getType[1]) & "By" & fieldNames.mapIt(it.capitalize).join("And"))

  # Create proc skeleton
  result = quote do:
    proc `procName`*(db: `dbType`): seq[`modelType`] =
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

  # Add the call params to the inner procedure call
  # result[6][0][1][0][1] is
  #   ProcDef -> [6]: StmtList (body) -> [0]: Call ->
  #     [1]: StmtList (withConn body) -> [0]: Asgn (result =) ->
  #     [1]: Call (inner findRecords invocation)
  result[6][0][1][0][1].add(callParams)

macro generateProcsForFieldLookups*(dbType: type, modelsAndFields: openarray[tuple[t: type, fields: seq[string]]]): untyped =
  result = newStmtList()

  for i in modelsAndFields:
    var modelType = i[1][0]
    let fieldNames = i[1][1][1].mapIt($it)

    let procName = ident("find" & $modelType & "sBy" & fieldNames.mapIt(it.capitalize).join("And"))

    # Create proc skeleton
    let procDefAST = quote do:
      proc `procName`*(db: `dbType`): seq[`modelType`] =
        db.withConn: result = findRecordsBy(conn, `modelType`)

    var callParams = quote do: @[]

    # Add dynamic parameters for the proc definition and inner proc call
    for n in fieldNames:
      let paramTuple = newNimNode(nnkPar)
      paramTuple.add(newColonExpr(ident("field"), newLit(identNameToDb(n))))
      paramTuple.add(newColonExpr(ident("value"), ident(n)))

      procDefAST[3].add(newIdentDefs(ident(n), ident("string")))
      callParams[1].add(paramTuple)

    procDefAST[6][0][1][0][1].add(callParams)

    result.add procDefAST

proc initPool*(
    connect: proc(): DbConn,
    poolSize = 10,
    hardCap = false,
    healthCheckQuery = "SELECT 'true' AS alive"): DbConnPool =
  ## Initialize a new DbConnPool.
  ##
  ## * `connect` must be a factory which creates a new `DbConn`
  ## * `poolSize` sets the desired capacity of the connection pool.
  ## * `hardCap` defaults to `false`.
  ##
  ##   When `false`, the pool can grow beyond the configured capacity, but will
  ##   release connections down to the its capacity (no less than `poolSize`).
  ##
  ##   When `true` the pool will not create more than its configured capacity.
  ##   It a connection is requested, none are free, and the pool is at
  ##   capacity, this will result in an Error being raised.
  ## * `healthCheckQuery` should be a simple and fast SQL query that the pool
  ##   can use to test the liveliness of pooled connections.

  initDbConnPool(DbConnPoolConfig(
    connect: connect,
    poolSize: poolSize,
    hardCap: hardCap,
    healthCheckQuery: healthCheckQuery))

template inTransaction*(db: DbConnPool, body: untyped) =
  pool.withConn(db):
    conn.exec(sql"BEGIN TRANSACTION")
    try:
      body
      conn.exec(sql"COMMIT")
    except:
      conn.exec(sql"ROLLBACK")
      raise getCurrentException()
