import std/[db_postgres, db_sqlite]

type DbConnType* = db_postgres.DbConn or db_sqlite.DbConn
