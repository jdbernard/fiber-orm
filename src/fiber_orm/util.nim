# Fiber ORM
#
# Copyright 2019 Jonathan Bernard <jonathan@jdbernard.com>

## Utility methods used internally by Fiber ORM.
import std/[json, macros, options, sequtils, strutils, times, unicode]
import uuids

import std/nre except toSeq

type
  MutateClauses* = object
    ## Data structure to hold information about the clauses that should be
    ## added to a query. How these clauses are used will depend on the query.
    ## This common data structure provides the information needed to create
    ## WHERE clauses, UPDATE clauses, etc.
    columns*: seq[string]
    placeholders*: seq[string]
    values*: seq[string]

const ISO_8601_FORMATS = @[
  "yyyy-MM-dd'T'HH:mm:ssz",
  "yyyy-MM-dd'T'HH:mm:sszzz",
  "yyyy-MM-dd'T'HH:mm:ss'.'fffzzz",
  "yyyy-MM-dd HH:mm:ssz",
  "yyyy-MM-dd HH:mm:sszzz",
  "yyyy-MM-dd HH:mm:ss'.'fffzzz"
]

proc parseIso8601(val: string): DateTime =
  var errString = ""
  for df in ISO_8601_FORMATS:
    try: return val.parse(df)
    except: errString &= "\n" & getCurrentExceptionMsg()
  raise newException(Exception, "Could not parse date. Tried:" & errString)

proc formatIso8601(d: DateTime): string =
  return d.format(ISO_8601_FORMATS[2])


# TODO: more complete implementation
# see https://github.com/blakeembrey/pluralize
proc pluralize*(name: string): string =
  ## Return the plural form of the given name.
  if name[^2..^1] == "ey": return name[0..^3] & "ies"
  if name[^1] == 'y': return name[0..^2] & "ies"
  return name & "s"

macro modelName*(model: object): string =
  ## For a given concrete record object, return the name of the `model class`_
  return newStrLitNode($model.getTypeInst)

macro modelName*(modelType: type): string =
  ## Get the name of a given `model class`_
  return newStrLitNode($modelType.getType[1])

proc identNameToDb*(name: string): string =
  ## Map a Nim identifier name to a DB name. See the `rules for name mapping`_
  ##
  ## TODO link above
  const UNDERSCORE_RUNE = "_".toRunes[0]
  let nameInRunes = name.toRunes
  var prev: Rune
  var resultRunes = newSeq[Rune]()

  for cur in nameInRunes:
    if resultRunes.len == 0:
      resultRunes.add(toLower(cur))
    elif isLower(prev) and isUpper(cur):
      resultRunes.add(UNDERSCORE_RUNE)
      resultRunes.add(toLower(cur))
    else: resultRunes.add(toLower(cur))

    prev = cur

  return $resultRunes

proc dbNameToIdent*(name: string): string =
  ## Map a DB name to a Nim identifier name. See the `rules for name mapping`_
  let parts = name.split("_")
  return @[parts[0]].concat(parts[1..^1].mapIt(capitalize(it))).join("")

proc tableName*(modelType: type): string =
  ## Get the `table name`_ for a given `model class`_
  return pluralize(modelName(modelType).identNameToDb)

proc tableName*[T](rec: T): string =
  ## Get the `table name`_ for a given record.
  return pluralize(modelName(rec).identNameToDb)

proc dbFormat*(s: string): string =
  ## Format a string for inclusion in a SQL Query.
  return s

proc dbFormat*(dt: DateTime): string =
  ## Format a DateTime for inclusion in a SQL Query.
  return dt.formatIso8601

proc dbFormat*[T](list: seq[T]): string =
  ## Format a `seq` for inclusion in a SQL Query.
  return "{" & list.mapIt(dbFormat(it)).join(",") & "}"

proc dbFormat*[T](item: T): string =
  ## For all other types, fall back on a defined `$` function to create a
  ## string version of the value we can include in an SQL query>
  return $item

type DbArrayParseState = enum
  expectStart, inQuote, inVal, expectEnd

proc parsePGDatetime*(val: string): DateTime =
  ## Parse a Postgres datetime value into a Nim DateTime object.

  const PG_TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:sszz",
    "yyyy-MM-dd'T'HH:mm:sszz",
    "yyyy-MM-dd HH:mm:ss'.'fff",
    "yyyy-MM-dd'T'HH:mm:ss'.'fff",
    "yyyy-MM-dd HH:mm:ss'.'fffzz",
    "yyyy-MM-dd'T'HH:mm:ss'.'fffzz",
    "yyyy-MM-dd HH:mm:ss'.'fffzzz",
    "yyyy-MM-dd'T'HH:mm:ss'.'fffzzz",
  ]

  var correctedVal = val;

  # PostgreSQL will truncate any trailing 0's in the millisecond value leading
  # to values like `2020-01-01 16:42.3+00`. This cannot currently be parsed by
  # the standard times format as it expects exactly three digits for
  # millisecond values. So we have to detect this and pad out the millisecond
  # value to 3 digits.
  let PG_PARTIAL_FORMAT_REGEX = re"(\d{4}-\d{2}-\d{2}( |'T')\d{2}:\d{2}:\d{2}\.)(\d{1,2})(\S+)?"
  let match = val.match(PG_PARTIAL_FORMAT_REGEX)

  if match.isSome:
    let c = match.get.captures
    if c.toSeq.len == 2: correctedVal = c[0] & alignLeft(c[2], 3, '0')
    else: correctedVal = c[0] & alignLeft(c[2], 3, '0') & c[3]

  var errStr = ""

  # Try to parse directly using known format strings.
  for df in PG_TIMESTAMP_FORMATS:
    try: return correctedVal.parse(df)
    except: errStr &= "\n\t" & getCurrentExceptionMsg()

  raise newException(ValueError, "Cannot parse PG date. Tried:" & errStr)

proc parseDbArray*(val: string): seq[string] =
  ## Parse a Postgres array column into a Nim seq[string]
  result = newSeq[string]()

  var parseState = DbArrayParseState.expectStart
  var curStr = ""
  var idx = 1
  var sawEscape = false

  while idx < val.len - 1:
    var curChar = val[idx]
    idx += 1

    case parseState:

      of expectStart:
        if curChar == ' ': continue
        elif curChar == '"':
          parseState = inQuote
          continue
        else:
          parseState = inVal

      of expectEnd:
        if curChar == ' ': continue
        elif curChar == ',':
          result.add(curStr)
          curStr = ""
          parseState = expectStart
          continue

      of inQuote:
        if curChar == '"' and not sawEscape:
          parseState = expectEnd
          continue

      of inVal:
        if curChar == '"' and not sawEscape:
          raise newException(ValueError, "Invalid DB array value (cannot have '\"' in the middle of an unquoted string).")
        elif curChar == ',':
          result.add(curStr)
          curStr = ""
          parseState = expectStart
          continue

    # if we saw an escaped \", add just the ", otherwise add both
    if sawEscape:
      if curChar != '"': curStr.add('\\')
      curStr.add(curChar)
      sawEscape = false

    elif curChar == '\\':
      sawEscape = true

    else: curStr.add(curChar)

  if not (parseState == inQuote) and curStr.len > 0:
    result.add(curStr)

func createParseStmt*(t, value: NimNode): NimNode =
  ## Utility method to create the Nim cod required to parse a value coming from
  ## the a database query. This is used by functions like `rowToModel` to parse
  ## the dataabase columns into the Nim object fields.

  if t.typeKind == ntyObject:

    if t.getTypeInst == Option.getType:
      var innerType = t.getTypeImpl[2][0] # start at the first RecList
      # If the value is a non-pointer type, there is another inner RecList
      if innerType.kind == nnkRecList: innerType = innerType[0]
      innerType = innerType[1] # now we can take the field type from the first symbol

      let parseStmt = createParseStmt(innerType, value)
      result = quote do:
        if `value`.len == 0:  none[`innerType`]()
        else:                 some(`parseStmt`)

    elif t.getType == UUID.getType:
      result = quote do: parseUUID(`value`)

    elif t.getType == DateTime.getType:
      result = quote do: parsePGDatetime(`value`)

    else: error "Unknown value object type: " & $t.getTypeInst

  elif t.typeKind == ntyGenericInst:

    if t.kind == nnkBracketExpr and
       t.len > 0 and
       t[0] == Option.getType:

      var innerType = t.getTypeInst[1]
      let parseStmt = createParseStmt(innerType, value)
      result = quote do:
        if `value`.len == 0:  none[`innerType`]()
        else:                 some(`parseStmt`)

    else: error "Unknown generic instance type: " & $t.getTypeInst

  elif t.typeKind == ntyRef:

    if $t.getTypeInst == "JsonNode":
      result = quote do: parseJson(`value`)

    else:
      error "Unknown ref type: " & $t.getTypeInst

  elif t.typeKind == ntySequence:
    let innerType = t[1]

    let parseStmts = createParseStmt(innerType, ident("it"))

    result = quote do: parseDbArray(`value`).mapIt(`parseStmts`)

  elif t.typeKind == ntyString:
    result = quote do: `value`

  elif t.typeKind == ntyInt:
    result = quote do: parseInt(`value`)

  elif t.typeKind == ntyFloat:
    result = quote do: parseFloat(`value`)

  elif t.typeKind == ntyBool:
    result = quote do: "true".startsWith(`value`.toLower)

  elif t.typeKind == ntyEnum:
    let innerType = t.getTypeInst
    result = quote do: parseEnum[`innerType`](`value`)

  else:
    error "Unknown value type: " & $t.typeKind

func fields(t: NimNode): seq[tuple[fieldIdent: NimNode, fieldType: NimNode]] =
  #[
  debugEcho "T: " & t.treeRepr
  debugEcho "T.kind: " & $t.kind
  debugEcho "T.typeKind: " & $t.typeKind
  debugEcho "T.GET_TYPE[1]: " & t.getType[1].treeRepr
  debugEcho "T.GET_TYPE[1].kind: " & $t.getType[1].kind
  debugEcho "T.GET_TYPE[1].typeKind: " & $t.getType[1].typeKind

  debugEcho "T.GET_TYPE: " & t.getType.treeRepr
  debugEcho "T.GET_TYPE[1].GET_TYPE: " & t.getType[1].getType.treeRepr
  ]#

  # Get the object type AST, with base object (if present) and record list.
  var objDefAst: NimNode
  if t.typeKind == ntyObject: objDefAst = t.getType
  elif t.typeKind == ntyTypeDesc:
    # In this case we have a type AST that is like:
    # BracketExpr
    #   Sym "typeDesc"
    #   Sym "ModelType"
    objDefAst = t.
      getType[1].         # get the Sym "ModelType"
      getType             # get the object definition type

    if objDefAst.kind != nnkObjectTy:
      error ("unable to enumerate the fields for model type '$#', " &
        "tried to resolve the type of the provided symbol to an object " &
        "definition (nnkObjectTy) but got a '$#'.\pAST:\p$#") % [
          $t, $objDefAst.kind, objDefAst.treeRepr ]
  else:
    error ("unable to enumerate the fields for model type '$#', " &
      "expected a symbol with type ntyTypeDesc but got a '$#'.\pAST:\p$#") % [
        $t, $t.typeKind, t.treeRepr ]

  # At this point objDefAst should look something like:
  # ObjectTy
  #   Empty
  #   Sym "BaseObject"" | Empty
  #   RecList
  #     Sym "field1"
  #     Sym "field2"
  #     ...

  if objDefAst[1].kind == nnkSym:
    # We have a base class symbol, let's recurse and try and resolve the fields
    # for the base class
    for fieldDef in objDefAst[1].fields: result.add(fieldDef)

  for fieldDef in objDefAst[2].children:
    # objDefAst[2] is a RecList of
    # ignore AST nodes that are not field definitions
    if fieldDef.kind == nnkIdentDefs: result.add((fieldDef[0], fieldDef[1]))
    elif fieldDef.kind == nnkSym: result.add((fieldDef, fieldDef.getTypeInst))
    else: error "unknown object field definition AST: $#" % $fieldDef.kind

template walkFieldDefs*(t: NimNode, body: untyped) =
  ## Iterate over every field of the given Nim object, yielding and defining
  ## `fieldIdent` and `fieldType`, the name of the field as a Nim Ident node
  ## and the type of the field as a Nim Type node respectively.
  for (fieldIdent {.inject.}, fieldType {.inject.}) in t.fields: body

#[ TODO: replace walkFieldDefs with things like this:
func columnNamesForModel*(modelType: typedesc): seq[string] =
  modelType.fields.mapIt(identNameToDb($it[0]))
]#

macro columnNamesForModel*(modelType: typed): seq[string] =
  ## Return the column names corresponding to the the fields of the given
  ## `model class`_
  var columnNames = newSeq[string]()

  modelType.walkFieldDefs:
    columnNames.add(identNameToDb($fieldIdent))

  result = newLit(columnNames)

macro rowToModel*(modelType: typed, row: seq[string]): untyped =
  ## Return a new Nim model object of type `modelType` populated with the
  ## values returned in the given database `row`

  # Create the object constructor AST node
  result = newNimNode(nnkObjConstr).add(modelType)

  # Create new colon expressions for each of the property initializations
  var idx = 0
  modelType.walkFieldDefs:
    let itemLookup = quote do: `row`[`idx`]
    result.add(newColonExpr(
      fieldIdent,
      createParseStmt(fieldType, itemLookup)))
    idx += 1

#[
macro listFields*(t: typed): untyped =
  var fields: seq[tuple[n: string, t: string]] = @[]
  t.walkFieldDefs:
    if fieldDef.kind == nnkSym: fields.add((n: $fieldIdent, t: fieldType.repr))
    else: fields.add((n: $fieldIdent, t: $fieldType))

  result = newLit(fields)
]#

proc typeOfColumn*(modelType: NimNode, colName: string): NimNode =
  ## Given a model type and a column name, return the Nim type for that column.
  modelType.walkFieldDefs:
    if $fieldIdent != colName: continue

    if fieldType.typeKind == ntyObject:

      if fieldType.getType == UUID.getType: return ident("UUID")
      elif fieldType.getType == DateTime.getType: return ident("DateTime")
      elif fieldType.getType == Option.getType: return ident("Option")
      else: error "Unknown column type: " & $fieldType.getTypeInst

    else: return fieldType

  raise newException(Exception,
    "model of type '" & $modelType & "' has no column named '" & colName & "'")

proc isEmpty(val: int): bool = return val == 0
proc isEmpty(val: UUID): bool = return val.isZero
proc isEmpty(val: string): bool = return val.isEmptyOrWhitespace
proc isEmpty[T](val: Option[T]): bool = return val.isNone

macro populateMutateClauses*(t: typed, newRecord: bool, mc: var MutateClauses): untyped =
  ## Given a record type, create the datastructure used to generate SQL clauses
  ## for the fields of this record type.

  result = newStmtList()

  # iterate over all the object's fields
  t.walkFieldDefs:

      # grab the field, it's string name, and it's type
      let fieldName = $fieldIdent

      # We only add clauses for the ID field if we're creating a new record and
      # the caller provided a value..
      if fieldName == "id":
        result.add quote do:
          if `newRecord` and not `t`.id.isEmpty:
            `mc`.columns.add(identNameToDb(`fieldName`))
            `mc`.placeholders.add("?")
            `mc`.values.add(dbFormat(`t`.`fieldIdent`))

      # if we're looking at an optional field, add logic to check for presence
      elif fieldType.kind == nnkBracketExpr and
           fieldType.len > 0 and
           fieldType[0] == Option.getType:

        result.add quote do:
          `mc`.columns.add(identNameToDb(`fieldName`))
          if isSome(`t`.`fieldIdent`):
            `mc`.placeholders.add("?")
            `mc`.values.add(dbFormat(`t`.`fieldIdent`.get))
          else:
            `mc`.placeholders.add("NULL")

      # otherwise assume we can convert and go ahead.
      else:
        result.add quote do:
          `mc`.columns.add(identNameToDb(`fieldName`))
          `mc`.placeholders.add("?")
          `mc`.values.add(dbFormat(`t`.`fieldIdent`))

## .. _model class: ../fiber_orm.html#objectminusrelational-modeling-model-class
## .. _rules for name mapping: ../fiber_orm.html
## .. _table name: ../fiber_orm.html
