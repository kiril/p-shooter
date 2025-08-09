import uuid from 'react-native-uuid'
import type PSHDatabase from './PSHDatabase'
import PSHRef from './PSHRef'
import PSHDeferredWrite from './PSHDeferredWrite'
import PSHEvent, { PSHEventType } from './PSHEvent'
import PSHDatabaseQuery from './PSHDatabaseQuery'
import Pea from './Pea'
import { PSHPK } from './PSHPK'


export default class PSHCollection {
  db: PSHDatabase
  name: string
  private _initialized = 0
  private _initializePromise: Promise<void>|null = null

  constructor(db: PSHDatabase, name: string) {
    // debug('PSHCollection.constructor', db.dbName, name)
    this.db = db
    this.name = name
    this.initialize()
  }

  get qualifiedName() {
    return `${this.db.dbName.split('.')[0]}.${this.name}`
  }

  get initialized() {
    return this._initialized > 0
  }

  ref(id?: string): PSHRef {
    id = id || uuid.v4() as string
    return new PSHRef(this, id)
  }

  async initialize() {
    let promise = this._initializePromise
    let outdated = false
    if (promise) {
      await promise
      if (this._initialized < this.db.initialized) {
        outdated = true
      }
    }
    if (!promise || outdated) {
      this._initializePromise = promise = new Promise<void>((resolve, reject) => {
        if (this._initialized > this.db.initialized) {
          // debug('PSHCollection.initialize/SKIP', this.name, `@${this.db.dbName}`)
          return
        }
        this.db.create(this.name)
          .then(() => {
            const d = Date.now()
            console.log('PSHCollection.initialize/success', this.qualifiedName, `db:${this.db.initialized}`, `this:${this._initialized}`, `now:${d}`)
            this._initialized = d
          })
          .then(resolve)
          .catch(e => { 
            console.error('PSHCollection.initialize/fail', this.qualifiedName, e)
            reject(e) 
          })

      })
    }
    return promise
  }

  async describe() {
    await this.initialize()
    return this.db.describe(this.name)
  }

  async count() {
    await this.initialize()
    return this.db.count(this.name)
  }

  async drop() {
    await this.initialize()
    await this.db.drop(this.name)
  }

  async all<Data extends Pea>(): Promise<Data[]> {
    await this.initialize()
    return this.db.all(this.name)
  }

  async get<Data extends Pea>(id: string): Promise<Data|null> {
    await this.initialize()
    return this.db.get(this.name, id)
  }

  async findOne<T extends Pea>(query: PSHDatabaseQuery): Promise<T|null> {
    await this.initialize()
    return this.db.findOne(this.name, query)
  }

  async find<Data extends Pea>(query: PSHDatabaseQuery): Promise<Data[]> {
    await this.initialize()
    return this.db.find(this.name, query)
  }

  async dateSaved(id: string) {
    await this.initialize()
    return this.db.dateSaved(this.name, id)
  }

  toWrite<Data extends Pea>(id: string, ob: Data): PSHDeferredWrite {
    return this.db.toWrite(this.name, id, ob)
  }

  async save<Data extends Pea>(ob: Data, id?: string): Promise<string> {
    await this.initialize()
    return this.db.save(this.name, ob, id)
  }

  async update(id: string, updates: Record<string,unknown>) {
    await this.initialize()
    const existing = await this.get(id)
    if (!existing) {
      throw Error(`PSHCollection.update: ${this.qualifiedName} missing value for id ${id}`)
    }
    return this.save({ ...existing, ...updates }, id)
  }

  async delete(id: string) {
    await this.initialize()
    await this.db.delete(this.name, id)
  }

  async deleteOne(query: PSHDatabaseQuery) {
    await this.initialize()
    await this.db.deleteOne(this.name, query)
  }

  async wipe() {
    await this.initialize()
    await this.db.wipe(this.name)
  }

  async on<DataType extends Pea=Pea, EventType extends PSHEvent<DataType>=PSHEvent<DataType>>(type: PSHEventType, call: (event: EventType) => void) {
    return this.initialize().then(() => this.db.events.register({ col: this.name, on: type, call }))
  }

  async onDoc<DataType extends Pea=Pea>(id: PSHPK, type: PSHEventType, call: (object: DataType) => void): Promise<() => void> {
    return this.on<DataType>(type, (event) => {
      if (event.id === id && event.after) {
        try {
          call(event.after)
        } catch (error) {
          console.error('PSHCollection.onDoc callback error:', error)
        }
      }
    })
  }

  async track(on: PSHEventType) {
    await this.initialize()
    await this.db.events.track(this.name, on)
  }
}