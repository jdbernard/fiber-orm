import json, macros, options, sequtils, strutils, times, timeutils, unicode,
  uuids

import nre except toSeq

const UNDERSCORE_RUNE = "_".toRunes[0]
const PG_TIMESTAMP_FORMATS = [
  "yyyy-MM-dd HH:mm:sszz",
  "yyyy-MM-dd HH:mm:ss'.'fffzz"
]

var PG_PARTIAL_FORMAT_REGEX = re"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.)(\d{1,3})(\S+)?"

type
  MutateClauses* = object
    columns*: seq[string]
    placeholders*: seq[string]
    values*: seq[string]

# TODO: more complete implementation
# see https://github.com/blakeembrey/pluralize
proc pluralize*(name: string): string =
  if name[^2..^1] == "ey": return name[0..^3] & "ies"
  if name[^1] == 'y': return name[0..^2] & "ies"
  return name & "s"

macro modelName*(model: object): string =
  return newStrLitNode($model.getTypeInst)

macro modelName*(modelType: type): string =
  return newStrLitNode($modelType.getType[1])

proc identNameToDb*(name: string): string =
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
  let parts = name.split("_")
  return @[parts[0]].concat(parts[1..^1].mapIt(capitalize(it))).join("")

proc tableName*(modelType: type): string =
  return pluralize(modelName(modelType).identNameToDb)

proc tableName*[T](rec: T): string =
  return pluralize(modelName(rec).identNameToDb)

proc dbFormat*(s: string): string = return s

proc dbFormat*(dt: DateTime): string = return dt.formatIso8601

proc dbFormat*[T](list: seq[T]): string =
  return "{" & list.mapIt(dbFormat(it)).join(",") & "}"

proc dbFormat*[T](item: T): string = return $item

type DbArrayParseState = enum
  expectStart, inQuote, inVal, expectEnd

proc parsePGDatetime*(val: string): DateTime =
  var errStr = ""

  # Try to parse directly using known format strings.
  for df in PG_TIMESTAMP_FORMATS:
    try: return val.parse(df)
    except: errStr &= "\n\t" & getCurrentExceptionMsg()

  # PostgreSQL will truncate any trailing 0's in the millisecond value leading
  # to values like `2020-01-01 16:42.3+00`. This cannot currently be parsed by
  # the standard times format as it expects exactly three digits for
  # millisecond values. So we have to detect this and pad out the millisecond
  # value to 3 digits.
  let match = val.match(PG_PARTIAL_FORMAT_REGEX)
  if match.isSome:
    let c = match.get.captures
    try:
      let corrected = c[0] & alignLeft(c[1], 3, '0') & c[2]
      return corrected.parse(PG_TIMESTAMP_FORMATS[1])
    except:
      errStr &= "\n\t" & PG_TIMESTAMP_FORMATS[1] &
        " after padding out milliseconds to full 3-digits"

  raise newException(ValueError, "Cannot parse PG date. Tried:" & errStr)

proc parseDbArray*(val: string): seq[string] =
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

proc createParseStmt*(t, value: NimNode): NimNode =

  #echo "Creating parse statment for ", t.treeRepr
  if t.typeKind == ntyObject:

    if t.getType == UUID.getType:
      result = quote do: parseUUID(`value`)

    elif t.getType == DateTime.getType:
      result = quote do: parsePGDatetime(`value`)

    elif t.getTypeInst == Option.getType:
      var innerType = t.getTypeImpl[2][0] # start at the first RecList
      # If the value is a non-pointer type, there is another inner RecList
      if innerType.kind == nnkRecList: innerType = innerType[0]
      innerType = innerType[1] # now we can take the field type from the first symbol

      let parseStmt = createParseStmt(innerType, value)
      result = quote do:
        if `value`.len == 0:  none[`innerType`]()
        else:                 some(`parseStmt`)

    else: error "Unknown value object type: " & $t.getTypeInst

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

template walkFieldDefs*(t: NimNode, body: untyped) =
  let tTypeImpl = t.getTypeImpl

  var nodeToItr: NimNode
  if tTypeImpl.typeKind == ntyObject: nodeToItr = tTypeImpl[2]
  elif tTypeImpl.typeKind == ntyTypeDesc: nodeToItr = tTypeImpl.getType[1].getType[2]
  else: error $t & " is not an object or type desc (it's a " & $tTypeImpl.typeKind & ")."

  for fieldDef {.inject.} in nodeToItr.children:
    # ignore AST nodes that are not field definitions
    if fieldDef.kind == nnkIdentDefs:
      let fieldIdent {.inject.} = fieldDef[0]
      let fieldType {.inject.} = fieldDef[1]
      body

    elif fieldDef.kind == nnkSym:
      let fieldIdent {.inject.} = fieldDef
      let fieldType {.inject.} = fieldDef.getType
      body

macro columnNamesForModel*(modelType: typed): seq[string] =
  var columnNames = newSeq[string]()

  modelType.walkFieldDefs:
    columnNames.add(identNameToDb($fieldIdent))

  result = newLit(columnNames)

macro rowToModel*(modelType: typed, row: seq[string]): untyped =

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

macro listFields*(t: typed): untyped =
  var fields: seq[tuple[n: string, t: string]] = @[]
  t.walkFieldDefs:
    if fieldDef.kind == nnkSym: fields.add((n: $fieldIdent, t: fieldType.repr))
    else: fields.add((n: $fieldIdent, t: $fieldType))

  result = newLit(fields)

proc typeOfColumn*(modelType: NimNode, colName: string): NimNode =
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
proc isEmpty(val: string): bool = return val.isNilOrWhitespace

macro populateMutateClauses*(t: typed, newRecord: bool, mc: var MutateClauses): untyped =

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
