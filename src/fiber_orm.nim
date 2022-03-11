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

type NotFoundError* = object of CatchableError

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
  let sqlStmt = "DELETE FROM " & tableName(modelType) & " WHERE id = ?"
  log().debug "deleteRecord: [" & sqlStmt & "] id: " & $id
  db.tryExec(sql(sqlStmt), $id)

proc deleteRecord*[T](db: DbConn, rec: T): bool =
  let sqlStmt = "DELETE FROM " & tableName(rec) & " WHERE id = ?"
  log().debug "deleteRecord: [" & sqlStmt & "] id: " & $rec.id
  return db.tryExec(sql(sqlStmt), $rec.id)

template getRecord*(db: DbConn, modelType: type, id: typed): untyped =
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
  let sqlStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType) &
    " WHERE " & whereClause

  log().debug "findRecordsWhere: [" & sqlStmt & "] values: (" & values.join(", ") & ")"
  db.getAllRows(sql(sqlStmt), values).mapIt(rowToModel(modelType, it))

template getAllRecords*(db: DbConn, modelType: type): untyped =
  let sqlStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType)

  log().debug "getAllRecords: [" & sqlStmt & "]"
  db.getAllRows(sql(sqlStmt)).mapIt(rowToModel(modelType, it))

template findRecordsBy*(db: DbConn, modelType: type, lookups: seq[tuple[field: string, value: string]]): untyped =
  let sqlStmt =
    "SELECT " & columnNamesForModel(modelType).join(",") &
    " FROM " & tableName(modelType) &
    " WHERE " & lookups.mapIt(it.field & " = ?").join(" AND ")
  let values = lookups.mapIt(it.value)

  log().debug "findRecordsBy: [" & sqlStmt & "] values (" & values.join(", ") & ")"
  db.getAllRows(sql(sqlStmt), values).mapIt(rowToModel(modelType, it))

macro generateProcsForModels*(dbType: type, modelTypes: openarray[type]): untyped =
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
