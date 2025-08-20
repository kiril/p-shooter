import { isArray } from "underscore"
import PSHDatabaseQuery, { PSHDatabaseQueryCondition, PSHDatabaseQueryOperator, PSHDatabaseQueryValue } from "./PSHDatabaseQuery"
import Pea from "./Pea"

export type PSHDataType = 'V8'|'V16'|'V32'|'REAL'|'INT'|'BOOLEAN'|'BLOB'|'TEXT'

let _debug = false
export const setDebug = (debug: boolean) => _debug = debug
export const maybeLog = (...args: any[]) => { if (_debug) console.log(...args) }
export const maybeError = (...args: any[]) => { if (_debug) console.error(...args) }
export const maybeWarn = (...args: any[]) => { if (_debug) console.warn(...args) }

export const dataTypeToColumnType = (dt: PSHDataType) => {
  switch (dt) {
    case 'V8': return 'VARCHAR(8)'
    case 'V16': return 'VARCHAR(16)'
    case 'V32': return 'VARCHAR(32)'
    case 'REAL': return 'REAL'
    case 'INT': return 'INTEGER'
    case 'BOOLEAN': return 'INTEGER'
    case 'BLOB': return 'BLOB'
    case 'TEXT': return 'TEXT'
  }
}

export type PSHSQLQueryable = [string, Array<PSHDatabaseQueryValue>]

export function toSQLQueryable(colName: string, query: PSHDatabaseQuery): PSHSQLQueryable {
  const pairs = Object.entries(query).map(([k, v]) => ({ k, v }))
  const clause = ({ k, v }: { k: string, v: unknown }) => isArray(v) ? `${k} ${(v as [string, unknown])[0]} ?` : `${k} = ?`
  const arg = ({ v }: { k: string, v: PSHDatabaseQueryCondition }) => isArray(v) ? (v as [string, PSHDatabaseQueryValue])[1] : v
  const args = pairs.map(arg)
  const sql = `SELECT id, json, date FROM ${colName} WHERE ${pairs.map(clause).join(' AND ')}`
  return [sql, args]
}

export const matchesQuery = <Object extends Pea=Pea>(object: Object|undefined, query: PSHDatabaseQuery): boolean => {
  if (!object) return false
  return Object.entries(query).every(([queryKey, queryValue]) => {
    const objectValue = object[queryKey as keyof Object] as PSHDatabaseQueryValue|undefined
    if (isArray(queryValue)) {
      const [operator, queryOperand] = queryValue as [PSHDatabaseQueryOperator, PSHDatabaseQueryValue]
      switch (operator) {
        case '=': return objectValue === queryOperand
        case '>': return (objectValue as number) > (queryOperand as number)
        case '<': return (objectValue as number) < (queryOperand as number)
        case '>=': return (objectValue as number) >= (queryOperand as number)
        case '<=': return (objectValue as number) <= (queryOperand as number)
        case '!=': return objectValue !== queryOperand
        case 'in': return objectValue && (queryValue as PSHDatabaseQueryValue[]).includes(objectValue)
        case 'not in': return !objectValue || !(queryValue as PSHDatabaseQueryValue[]).includes(objectValue)
        case 'like': return typeof objectValue === 'string' && objectValue.includes(queryOperand as string)
      }
    } else {
      return objectValue === queryValue
    }
  })
}