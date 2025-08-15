import SQLite, {
  SQLError, SQLResultSet,
  SQLStatementCallback, SQLStatementErrorCallback, SQLTransaction,
  WebsqlDatabase
} from 'react-native-sqlite-2'

import SQLColumnInfo from './types/sqlite/SQLColumnInfo'
import { maybeError, maybeLog, maybeWarn } from './shared'


interface SQLiteConnectionOptions {
  name: string
  version: string
  description: string
  size?: number
}

export default class PSHSQLiteWrapper {
  sqlDb: WebsqlDatabase
  constructor(db: WebsqlDatabase) {
    this.sqlDb = db
  }

  static connect({ name, version, description, size }: SQLiteConnectionOptions): Promise<PSHSQLiteWrapper> {
    return new Promise((resolve) => {
      const onConnect = (db: WebsqlDatabase) => resolve(new PSHSQLiteWrapper(db))
      size = typeof size === 'number' ? size : -1
      SQLite.openDatabase({ name, version, description, size  }, onConnect)
    })
  }

  async countTable(table: string): Promise<number> {
    const res = await this.query<{ count?: number }>(`SELECT count(*) as count FROM ${table}`)
    return res[0].count!
  }

  async count(sql: string): Promise<number> {
    const res = await this.query<{ count?: number, 'count(*)'?: number }>(sql)
    if (res.length !== 1) {
      return 0
    }
    return res[0].count || res[0]['count(*)'] || 0
  }

  async tables(): Promise<string[]> {
    const res = await this.query<{ name: string }>('SELECT name FROM sqlite_schema WHERE type="table" ORDER BY name')
    return res.map(r => r.name)
  }

  async describe(tableName: string) {
    const columns = await this.query<SQLColumnInfo>(`PRAGMA table_info(${tableName})`)
    return {
      name: tableName,
      columns
    }
  }

  async try(sql: string): Promise<boolean> {
    return new Promise<boolean>((resolve) => {
      const onSuccess: SQLStatementCallback = () => resolve(true)
      const onError: SQLStatementErrorCallback = (t, e) => {
        const thing = `${e}`
        if (!thing.includes('duplicate column name')) {
          maybeLog('PSHSQLiteWrapper.try: failure', e)
        } else {
          maybeLog('PHSQLiteWrapper.try: duplicate column name')
        }
        resolve(false)
        return true
      }
      this.sqlDb.transaction((tx: SQLTransaction) => {
        tx.executeSql(sql, [], onSuccess, onError)
      })
    })
  }

  async query<T>(sql: string, args?: Array<unknown>): Promise<T[]> {
    return new Promise<T[]>((resolve, reject) => {
      const onSuccess: SQLStatementCallback = (tx: SQLTransaction, results: SQLResultSet) => {
        const ret: T[] = []
        for (let i = 0; i < results.rows.length; i++) {
          ret.push(results.rows.item(i) as T)
        }
        resolve(ret)
      }
      const onError: SQLStatementErrorCallback = (tx: SQLTransaction, sqlError: SQLError) => {
        maybeError('PHSQLiteWrapper.query/onError', sql, sqlError)
        reject(sqlError)
        return true
      }
      this.sqlDb.transaction((tx: SQLTransaction) => {
        tx.executeSql(sql, args || [], onSuccess, onError)
      })
    })
  }

  async findOne<T>(sql: string, args?: Array<unknown>): Promise<T|null> {
    const matches = await this.query<T>(sql, args)
    return matches.length > 0 ? matches[0] : null
  }

  async get<T>(sql: string, args?: Array<unknown>): Promise<T|null> {
    const all = await this.query<T>(sql, args)
    if (all.length === 1) return all[0]
    if (all.length === 0) return null
    throw Error(`Expected 0 or 1 result, got ${all.length} ${sql}`)
  }

  async run(sql: string, args?: Array<unknown>): Promise<SQLResultSet> {
    return new Promise<SQLResultSet>((resolve, reject) => {
      const onSuccess: SQLStatementCallback = (tx: SQLTransaction, results: SQLResultSet) => {
        // log('PSHQLiteWrapper.run/onSuccess', sql, args, '->', results.rows.length, 'rows')
        resolve(results)
      }
      const onError: SQLStatementErrorCallback = (tx: SQLTransaction, e: SQLError) => {
        maybeError('PSHSQLiteWrapper.run/onError', sql, e)
        reject(e)
        return true
      }
      this.sqlDb.transaction((tx: SQLTransaction) => {
        tx.executeSql(sql, args || [], onSuccess, onError)
      })
    })
  }

  async insert<T>(sql: string, args?: Array<unknown>): Promise<T|null> {
    return new Promise<T|null>((resolve, reject) => {
      const onSuccess: SQLStatementCallback = (tx: SQLTransaction, results: SQLResultSet) => {
        if (results.rows.length === 1) {
          resolve(results.rows.item(0) as T)
        } else if (results.rows.length > 1) {
          maybeWarn('PSHSQLiteWrapper.insert got', results.rows.length, 'rows')
        }
        // log('PSHSQLiteWrapper.insert', results.rows, 'rows', results)
        resolve(null)
      }
      const onError: SQLStatementErrorCallback = (tx: SQLTransaction, sqlError: SQLError) => {
        maybeError('PSHSQLiteWrapper.insert/onError', sql, sqlError)
        reject(sqlError)
        return true
      }
      this.sqlDb.transaction((tx: SQLTransaction) => {
        tx.executeSql(sql, args || [], onSuccess, onError)
      })
    })
  }

  transaction(callback: (tx: SQLTransaction) => void) {
    return new Promise<void>((resolve0, reject0) => {
      const resolve = resolve0
      const reject = (e: any) => { maybeError('PSHSQLiteWrapper.transaction/reject', e); reject0(e) }
      this.sqlDb.transaction(callback, reject, resolve)
    })
  }
}