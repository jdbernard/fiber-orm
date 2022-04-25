import std/db_postgres, std/sequtils, std/strutils, std/sugar

import namespaced_logging


type
  DbConnPoolConfig* = object
    connect*: () -> DbConn
    poolSize*: int
    hardCap*: bool
    healthCheckQuery*: string

  PooledDbConn = ref object
    conn: DbConn
    id: int
    free: bool

  DbConnPool* = ref object
    conns: seq[PooledDbConn]
    cfg: DbConnPoolConfig
    lastId: int

var logNs {.threadvar.}: LoggingNamespace

template log(): untyped =
  if logNs.isNil: logNs = initLoggingNamespace(name = "fiber_orm/pool", level = lvlNotice)
  logNs

proc initDbConnPool*(cfg: DbConnPoolConfig): DbConnPool =
  log().debug("Initializing new pool (size: " & $cfg.poolSize)
  result = DbConnPool(
    conns: @[],
    cfg: cfg)

proc newConn(pool: DbConnPool): PooledDbConn =
  log().debug("Creating a new connection to add to the pool.")
  pool.lastId += 1
  let conn = pool.cfg.connect()
  result = PooledDbConn(
    conn: conn,
    id: pool.lastId,
    free: true)
  pool.conns.add(result)

proc maintain(pool: DbConnPool): void =
  log().debug("Maintaining pool. $# connections." % [$pool.conns.len])
  pool.conns.keepIf(proc (pc: PooledDbConn): bool =
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
    let toCull = freeConns[0..numToCull]
    pool.conns.keepIf((pc) => toCull.allIt(it.id != pc.id))
    for culled in toCull:
      try: culled.conn.close()
      except: discard ""
    log().debug(
      "Trimming pool size. Culled $# free connections. $# connections remaining." %
      [$toCull.len, $pool.conns.len])

proc take*(pool: DbConnPool): tuple[id: int, conn: DbConn] =
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

proc release*(pool: DbConnPool, connId: int): void =
  log().debug("Reclaiming released connaction $#" % [$connId])
  let foundConn = pool.conns.filterIt(it.id == connId)
  if foundConn.len > 0: foundConn[0].free = true

template withConn*(pool: DbConnPool, stmt: untyped): untyped =
  let (connId, conn {.inject.}) = take(pool)
  try: stmt
  finally: release(pool, connId)
