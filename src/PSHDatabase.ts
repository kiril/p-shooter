import { get, isArray, union, uniq } from 'underscore'
import uuid from 'react-native-uuid'

import SQLColumnInfo from './types/sqlite/SQLColumnInfo'

import PSHSQLiteWrapper from './PSHSQLiteWrapper'
import PSHTransaction from './PSHTransaction'
import PSHCollection from './PSHCollection'
import PSHIndexing, { PSHIndexSpec } from './PSHIndexing'
import PSHDeferredWrite from './PSHDeferredWrite'
import PSHEventEngine from './PSHEventEngine'
import PSHDatabaseQuery from './PSHDatabaseQuery'
import PSHColumnDef from './PSHColumnDef'
import Pea from './Pea'
import { PSHPK } from './PSHPK'


export default class PSHDatabase {
  private static _promises: Record<string,Promise<PSHDatabase>> = {}
  
  static async connect({ name, config }: { name: string, config?: PSHDatabaseConfig }) {
    let promise = this._promises[name]
    if (!promise) {
      this._promises[name] = promise = new Promise<PSHDatabase>((resolve, reject) => {
        console.log('PSHDatabase.connect', name)
        const description = `P-Shooter Backing Store ${name}`
        const params = { name: `${name}.db`, version: '1.0', description }
        PSHSQLiteWrapper.connect(params)
          .then(sqlDb => new PSHDatabase(sqlDb, config).initialize().then(resolve))
          .catch(reject)
      })
    }
    return promise
  }

  private readonly config: PSHDatabaseConfig
  readonly events: PSHEventEngine
  private _initialized = 0
  private collections: Record<string,PSHCollection> = {}

  get initialized() {
    return this._initialized
  }
  
  private constructor(readonly sqlDb: PSHSQLiteWrapper, config?: PSHDatabaseConfig) {
    this.config = { ...config }
    this.events = new PSHEventEngine(this.sqlDb)
  }

  get dbName() {
    return this.sqlDb.sqlDb._db._name
  }

  col(name: string) {
    if (!this.collections[name]) {
      // debug('PSHDatabase.col', name, `@${this.dbName}`)
      this.collections[name] = new PSHCollection(this, name)
    }
    return this.collections[name]
  }

  async count(colName: string) {
    return this.sqlDb.count(`SELECT count(*) FROM ${colName}`)
  }

  async describe(colName: string) {
    return this.sqlDb.describe(colName)
  }

  private async initialize(force?: boolean) {
    if (this._initialized === 0 || force) {
      console.log('PSHDatabase.initialize', this.dbName)
      await Promise.all(this.collectionsToInitialize().map(c => this.create(c)))
      await this.events.initialize()
      await this.events.start()
      this._initialized = Date.now()
    }
    return this
  }

  async reset() {
    await this.events.reset()
    const tables = await this.tables()
    let failedTables: string[] = []
    for (const table of tables.filter(t => t !== 'sqlite_sequence')) {
      await this.sqlDb.run(`DROP TABLE IF EXISTS ${table}`).catch(() => { if (!failedTables.includes(table)) { failedTables.push(table) }})
    }

    for (const table of failedTables.reverse()) {
      console.log('PSHDatabase.reset/retry drop', table)
      await this.sqlDb.run(`DROP TABLE IF EXISTS ${table}`)
        .then(() => failedTables = failedTables.filter(t => t !== table))
        .catch(() => console.warn('PSHDatabase.reset/failed drop', table))
    }
    this.collections = {}
    await this.initialize(true)
  }

  private collectionsToInitialize(): string[] {
    return this.config && this.config.indices ? Object.keys(this.config.indices) : []
  }

  private indicesForCollection(name: string) {
    return this.config && this.config.indices && this.config.indices[name] || []
  }

  private async columns(name: string): Promise<PSHColumnDef[]> {
    return this.sqlDb.query<SQLColumnInfo>(`PRAGMA table_info(${name})`).then(cols => cols.map(c => ({ name: c.name, type: c.type })))
  }

  async create(colName: string) {
    // const qName = this.qualified(colName)
    // log('PSHDatabase.create', qName)
    const createSQL = `CREATE TABLE IF NOT EXISTS ${colName} (id VARCHAR(32) PRIMARY KEY, json TEXT NOT NULL, date INTEGER NOT NULL)`
    await this.sqlDb.run(createSQL)

    const indices = this.indicesForCollection(colName)
    const existingColumns = await this.columns(colName)
    const indexColumns = uniq(union(...indices.map(PSHIndexing.toColumns)), false, col => col.name)
    console.log('PSHDatabase.create', this.dbName, colName, 'existing columns', existingColumns.map(c => c.name))
    console.log('PSHDatabase.create', this.dbName, colName, 'to-index columns', indexColumns.map(c => c.name))
    const addColumn = async (colDef: PSHColumnDef) => this.sqlDb.try(`ALTER TABLE ${colName} ADD COLUMN ${colDef.name}`)
    await Promise.all(indexColumns.filter(colDef => !existingColumns.find(c => c.name === colDef.name)).map(addColumn))
    const UNQ = (ix: PSHIndexSpec) => ix.unique ? 'UNIQUE' : ''
    await Promise.all(indices.map(ix => this.sqlDb.run(`CREATE ${UNQ(ix)} INDEX IF NOT EXISTS ${PSHIndexing.toIndex(ix, colName)}`)))
  }

  async drop(colName: string) {
    await this.sqlDb.run(`DROP TABLE IF EXISTS ${colName}`)
  }

  async all<Data extends Pea>(colName: string): Promise<Data[]> {
    const allJson = await this.sqlDb.query<Wrapped>(`SELECT id, json, date FROM ${colName}`)
    return allJson.map(unwrap<Data>).filter(x => !!x)
  }
  
  async get<Data extends Pea>(colName: string, id: string): Promise<Data|null> {
    return this.sqlDb.get<Wrapped>(`SELECT id, json, date FROM ${colName} WHERE id = ?`, [id]).then(x => x ? unwrap<Data>(x) : null)
  }

  async explain(colName: string, query: PSHDatabaseQuery) {
    const [querySql, args] = this.toSQLandArgs(colName, query)
    const explainQuery = `explain query plan ${querySql}`
    const ret = await this.sqlDb.query<object>(explainQuery, args)
    console.log('explain', colName, query, ret)
  }

  private toSQLandArgs(colName: string, query: PSHDatabaseQuery): [string, Array<unknown>] {
    const pairs = Object.entries(query).map(([k, v]) => ({ k, v }))
    const clause = ({ k, v }: { k: string, v: unknown }) => isArray(v) ? `${k} ${(v as [string, unknown])[0]} ?` : `${k} = ?`
    const arg = ({ v }: { k: string, v: unknown }) => isArray(v) ? (v as [string, unknown])[0] : v
    const args = pairs.map(arg)
    const sql = `SELECT id, json, date FROM ${colName} WHERE ${pairs.map(clause).join(' AND ')}`
    return [sql, args]
  }

  async findOne<Data extends Pea>(colName: string, query: PSHDatabaseQuery): Promise<Data|null> {
    const [sql, args] = this.toSQLandArgs(colName, query)
    const matches = await this.sqlDb.query<Wrapped>(sql, args)
      .then(rs => rs.map(unwrap<Data>))
      .catch(e => { console.error('PSHDatabase.findOne/error(query)', sql, e); throw e })
    if (matches.length > 1) {
      console.warn('PSHDatabase.findOne removing', matches.length-1, 'dupes for', colName, query)
      await Promise.all(matches.slice(1).map(t => this.delete(colName, t.id!)))
    }

    // debug('PSHDatabase.findOne', sql, args)
    return this.sqlDb.get<Wrapped>(sql, args).then(x => x ? unwrap<Data>(x) : null).catch(e => { console.error('PSHDatabase.findOne/error', sql, e); throw e })
  }

  async find<Data extends Pea>(colName: string, query: PSHDatabaseQuery): Promise<Data[]> {
    const [sql, args] = this.toSQLandArgs(colName, query)
    // log('PSHDatabase.find', sql, args)
    return this.sqlDb.query<Wrapped>(sql, args).then(res => res.map(unwrap<Data>)).catch(e => { console.error('PSHDatabase.find/error', sql, e); throw e })
  }

  async dateSaved(colName: string, id: PSHPK) {
    const record = await this.sqlDb.get<{ date: number }>(`SELECT date FROM ${colName} WHERE id = ?`, [id])
    return record ? record.date : null
  }

  async save<Data extends Pea>(colName: string, ob: Data, id?: PSHPK): Promise<PSHPK> {
    id = id || ob.id || (uuid.v4() as string)
    const { sql, args } = this.toWrite(colName, id, ob)
    await this.sqlDb.run(sql, args)
    return id
  }

  async delete(colName: string, id: PSHPK) {
    console.log('PSHDB: delete', { colName, id })
    await this.sqlDb.run(`DELETE FROM ${colName} WHERE id = ?`, [id])
  }

  async deleteOne(colName: string, query: PSHDatabaseQuery) {
    const keys = Object.keys(query)
    const args = keys.map(k => query[k])
    const sql = `DELETE FROM ${colName} WHERE ${keys.map(k => `${k} = ?`).join(' AND ')}`
    // debug('PSHDatabase.deleteOne/sql', sql, args)
    await this.sqlDb.run(sql, args)
  }

  async wipe(colName: string) {
    await this.sqlDb.try(`DELETE FROM ${colName}`)
  }

  toWrite<Data extends Pea>(colName: string, id: PSHPK, ob: Data): PSHDeferredWrite {
    const json = JSON.stringify({ ...ob, id })
    const now = Date.now()
    
    const indices = this.indicesForCollection(colName)
    const indexColumns = uniq(union(...indices.map(PSHIndexing.toColumns)), false, col => col.name)
    const indexedColumnValues: unknown[] = indexColumns.map(c => get(ob, PSHIndexing.fieldToIndexPath(c.name).split('.')))
    const allColumns = ['id', 'json', 'date'].concat(indexColumns.map(c => c.name))
    const allValues = ([id, json, now] as unknown[]).concat(indexedColumnValues)
    const onConflictKeys = ['json', 'date'].concat(indexColumns.map(c => c.name))
    const onConflictValues = ([json, now] as unknown[]).concat(indexedColumnValues)
    
    const sql = `INSERT INTO ${colName} (${allColumns.join(', ')}) VALUES (${new Array(allColumns.length).fill('?').join(', ')}) ON CONFLICT DO UPDATE SET ${onConflictKeys.map(k => `${k} = ?`).join(', ')}`
    const args = allValues.concat(onConflictValues)
    return { sql, args }
  }

  async tables(): Promise<string[]> {
    const res = await this.sqlDb.query<{ name: string }>('SELECT name FROM sqlite_schema WHERE type="table" ORDER BY name')
    return res.map(r => r.name)
  }

  transaction() {
    return new PSHTransaction(this.sqlDb)
  }
}

export interface PSHDatabaseConfig {
  indices?: Record<string, PSHIndexSpec[]>
}

const unwrap = <Data extends Pea>(wrapper: Wrapped): Data => ({ ...JSON.parse(wrapper.json), saved: wrapper.date, id: wrapper.id || 'WTAF' } as Data)

interface Wrapped {
  id: PSHPK
  json: string
  date: number
}