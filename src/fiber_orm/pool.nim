# Fiber ORM
#
# Copyright 2019 Jonathan Bernard <jonathan@jdbernard.com>

## Simple database connection pooling implementation compatible with Fiber ORM.

import std/[db_common, logging, sequtils, strutils, sugar]

from std/db_sqlite import getRow
from std/db_postgres import getRow

import namespaced_logging
import ./db_common as fiber_db_common

type
  DbConnPoolConfig*[D: DbConnType] = object
    connect*: () -> D   ## Factory procedure to create a new DBConn
    poolSize*: int      ## The pool capacity.

    hardCap*: bool      ## Is the pool capacity a hard cap?
                        ##
                        ## When `false`, the pool can grow beyond the
                        ## configured capacity, but will release connections
                        ## down to the its capacity (no less than `poolSize`).
                        ##
                        ## When `true` the pool will not create more than its
                        ## configured capacity.  It a connection is requested,
                        ## none are free, and the pool is at capacity, this
                        ## will result in an Error being raised.

    healthCheckQuery*: string ## Should be a simple and fast SQL query that the
                              ## pool can use to test the liveliness of pooled
                              ## connections.

  PooledDbConn[D: DbConnType] = ref object
    conn: D
    id: int
    free: bool

  DbConnPool*[D: DbConnType] = ref object
    ## Database connection pool
    conns: seq[PooledDbConn[D]]
    cfg: DbConnPoolConfig[D]
    lastId: int

var logNs {.threadvar.}: LoggingNamespace

template log(): untyped =
  if logNs.isNil: logNs = getLoggerForNamespace(namespace = "fiber_orm/pool", level = lvlNotice)
  logNs

proc initDbConnPool*[D: DbConnType](cfg: DbConnPoolConfig[D]): DbConnPool[D] =
  log().debug("Initializing new pool (size: " & $cfg.poolSize)
  result = DbConnPool[D](
    conns: @[],
    cfg: cfg)

proc newConn[D: DbConnType](pool: DbConnPool[D]): PooledDbConn[D] =
  log().debug("Creating a new connection to add to the pool.")
  pool.lastId += 1
  let conn = pool.cfg.connect()
  result = PooledDbConn[D](
    conn: conn,
    id: pool.lastId,
    free: true)
  pool.conns.add(result)

proc maintain[D: DbConnType](pool: DbConnPool[D]): void =
  log().debug("Maintaining pool. $# connections." % [$pool.conns.len])
  pool.conns.keepIf(proc (pc: PooledDbConn[D]): bool =
    if not pc.free: return true

    try:
      discard getRow(pc.conn, sql(pool.cfg.healthCheckQuery), [])
      return true
    except:
      try: pc.conn.close()  # try to close the connection
      except: discard ""
      return false
  )
  log().debug(
    "Pruned dead connections. $# connections remaining." %
    [$pool.conns.len])

  let freeConns = pool.conns.filterIt(it.free)
  if pool.conns.len > pool.cfg.poolSize and freeConns.len > 0:
    let numToCull = min(freeConns.len, pool.conns.len - pool.cfg.poolSize)

    if numToCull > 0:
      let toCull = freeConns[0..numToCull]
      pool.conns.keepIf((pc) => toCull.allIt(it.id != pc.id))
      for culled in toCull:
        try: culled.conn.close()
        except: discard ""
      log().debug(
        "Trimming pool size. Culled $# free connections. $# connections remaining." %
        [$toCull.len, $pool.conns.len])

proc take*[D: DbConnType](pool: DbConnPool[D]): tuple[id: int, conn: D] =
  ## Request a connection from the pool. Returns a DbConn if the pool has free
  ## connections, or if it has the capacity to create a new connection. If the
  ## pool is configured with a hard capacity limit and is out of free
  ## connections, this will raise an Error.
  ##
  ## Connections taken must be returned via `release` when the caller is
  ## finished using them in order for them to be released back to the pool.
  pool.maintain
  let freeConns = pool.conns.filterIt(it.free)

  log().debug(
    "Providing a new connection ($# currently free)." % [$freeConns.len])

  let reserved =
    if freeConns.len > 0: freeConns[0]
    else: pool.newConn()

  reserved.free = false
  log().debug("Reserve connection $#" % [$reserved.id])
  return (id: reserved.id, conn: reserved.conn)

proc release*[D: DbConnType](pool: DbConnPool[D], connId: int): void =
  ## Release a connection back to the pool.
  log().debug("Reclaiming released connaction $#" % [$connId])
  let foundConn = pool.conns.filterIt(it.id == connId)
  if foundConn.len > 0: foundConn[0].free = true

template withConn*[D: DbConnType](pool: DbConnPool[D], stmt: untyped): untyped =
  ## Convenience template to provide a connection from the pool for use in a
  ## statement block, automatically releasing that connnection when done.
  ##
  ## The provided connection is injected as the variable `conn` in the
  ## statement body.
  let (connId, conn {.inject.}) = take(pool)
  try: stmt
  finally: release(pool, connId)
