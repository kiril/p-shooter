import { isEmpty, uniq } from 'underscore'

import PSHEvent, { PSHEventType } from './PSHEvent'
import PSHSQLiteWrapper from './PSHSQLiteWrapper'
import PSHTrigger, { cursorName } from './PSHTrigger'
import { sleep } from './util'


interface EVTTable {
  name: string
  columns: EVTColumn[]
  indices: EVTIndex[]
}

interface EVTColumn {
  name: string
  type: string
}

interface EVTIndex {
  name: string
  on: (string|string[])[]
}


const Tables: Record<string,EVTTable> = {
  events: {
    name: '_events',
    columns: [
      { name: 'col', type: 'VARCHAR(32) NOT NULL' },
      { name: 'id', type: 'VARCHAR(32) NOT NULL' },
      { name: 'type', type: 'VARCHAR(32) NOT NULL' },
      { name: 'date', type: 'INTEGER NOT NULL' },
      { name: 'before', type: 'TEXT' },
      { name: 'after', type: 'TEXT' },
    ],
    indices: [
      { name: 'date_col_type', on: ['date', 'col', 'type'] },
    ]
  },
  cursors: {
    name: '_cursors',
    columns: [
      { name: 'name', type: 'VARCHAR(32) NOT NULL PRIMARY KEY' },
      { name: 'date', type: 'INTEGER NOT NULL' },
    ],
    indices: []
  }
}

const createSql = (table: EVTTable) => {
  return `CREATE TABLE IF NOT EXISTS ${table.name} (${table.columns.map(({ name, type }) => `${name} ${type}`).join(', ')})`
}

const indexSql = (table: EVTTable, ix: EVTIndex) => {
  const toField = (indexSpec: string|string[]) => typeof(indexSpec) === 'string' ? indexSpec : indexSpec.join(' ')
  return `CREATE INDEX IF NOT EXISTS ${ix.name} ON ${table.name} (${ix.on.map(toField).join(', ')})`
}


export default class PSHEventEngine {
  private initialized = false
  private triggerRunners: PSHTriggerRunner[] = []
  
  constructor(private sqlDb: PSHSQLiteWrapper) {
  }

  async initialize() {
    if (this.initialized) {
      return
    }

    const initializeTable = async (table: EVTTable) => {
      await this.sqlDb.run(createSql(table))
      if (table.indices.length > 0) {
        await Promise.all(table.indices.map(ix => indexSql(table, ix)).map(sql => this.sqlDb.run(sql)))
      }
    }

    await Promise.all(Object.values(Tables).map(initializeTable))
    console.log('PSHEventEngine.initialize/SUCCESS')
    this.initialized = true
  }

  async track(col: string, on: PSHEventType) {
    const statements: string[] = []
    switch (on) {
      case 'insert':
        statements.push(`CREATE TRIGGER IF NOT EXISTS ${col}_insert INSERT ON ${col} BEGIN INSERT INTO _events (col, id, type, date, before, after) VALUES ("${col}", new.id, "insert", unixepoch('subsec')*1000, NULL, new.json); END;`)
        break
      case 'update':
        statements.push(`CREATE TRIGGER IF NOT EXISTS ${col}_update UPDATE ON ${col} BEGIN INSERT INTO _events (col, id, type, date, before, after) VALUES ("${col}", old.id, "update", unixepoch('subsec')*1000, old.json, new.json); END;`)
        break
      case 'write':
        statements.push(`CREATE TRIGGER IF NOT EXISTS ${col}_write_insert INSERT ON ${col} BEGIN INSERT INTO _events (col, id, type, date, before, after) VALUES ("${col}", new.id, "write", unixepoch('subsec')*1000, NULL, new.json); END;`)
        statements.push(`CREATE TRIGGER IF NOT EXISTS ${col}_write_update UPDATE ON ${col} BEGIN INSERT INTO _events (col, id, type, date, before, after) VALUES ("${col}", old.id, "write", unixepoch('subsec')*1000, old.json, new.json); END;`)
        break
      case 'delete':
        statements.push(`CREATE TRIGGER IF NOT EXISTS ${col}_delete DELETE ON ${col} BEGIN INSERT INTO _events (col, id, type, date, before) VALUES ("${col}", old.id, "delete", unixepoch('subsec')*1000, old.json); END;`)
        break
    }
    await Promise.all(statements.map(s => this.sqlDb.run(s)))
  }

  private _registerPromises: Record<string,Promise<void>> = {}
  async register(trigger: PSHTrigger): Promise<void> {
    const name = cursorName(trigger)
    let promise = this._registerPromises[name]

    if (!promise) {
      this._registerPromises[name] = promise = new Promise<void>((resolve, reject) => {
        this.initialize()
          .then(() => {
            const runner = this.triggerRunners.find(r => r.name === name)
            if (runner) {
              console.log('PSHEE.register/FOUND', name, 'so not starting a new one')
              runner.trigger = trigger // updates the callback in case
              runner.start() // just in case
              resolve()
              return
            }
            console.log('PSHEE.register/NEW', name)
      
            this.track(trigger.col, trigger.on)
            // const { date } = await this.sqlDb.insert<{ date: number }>('INSERT INTO _cursors (name, date) VALUES (?, ?) ON CONFLICT (name) DO NOTHING RETURNING date', [name, Date.now()])
            return this.sqlDb.findOne<{ date: number, name: string }>('SELECT * FROM _cursors WHERE name = ?', [name])
          })
          .then(existing => {
            if (existing) {
              return existing.date
            } else {
              const date = Date.now()
              const sql = 'INSERT INTO _cursors (name, date) VALUES (?, ?) ON CONFLICT (name) DO NOTHING'
              return this.sqlDb.insert<{ date: number }>(sql, [name, date])
                .then(() => date)
            }
          })
          .then(date => {
            const runner = new PSHTriggerRunner(this.sqlDb, trigger, date)
            this.triggerRunners.push(runner)
            return runner.start()
          })
          .then(resolve)
          .catch(e => {
            console.error('PSHEE.register/ERROR', e)
            reject(e)
          })

      })
    }

    return promise
  }

  async reset() {
    await this.stop()
    this._registerPromises = {}
    this.triggerRunners = []
    this.initialized = false
  }

  async start() {
    await Promise.all(this.triggerRunners.map(runner => runner.start()))
  }

  async stop() {
    await Promise.all(this.triggerRunners.map(runner => runner.stop()))
  }
}

class PSHTriggerRunner {
  name: string
  private cursor: number|null = null
  private stopped = false

  constructor(private sqlDb: PSHSQLiteWrapper, public trigger: PSHTrigger, cursor?: number)  {
    this.name = cursorName(trigger)
    if (cursor) {
      this.cursor = cursor
    }
  }

  get isRunning() {
    return !this.stopped
  }

  private async fetchCursor() {
    if (this.cursor === null) {
      let date: number
      const existing = await this.sqlDb.findOne<{ date: number, name: string }>('SELECT * FROM _cursors WHERE name = ?', [this.name])
      if (existing) {
        date = existing.date
      } else {
        date = Date.now()
        await this.sqlDb.insert<{ date: number }>('INSERT INTO _cursors (name, date) VALUES (?, ?) ON CONFLICT (name) DO NOTHING', [this.name, date])
      }
      this.cursor = date
    }
  }

  private async writeCursor(cursor: number) {
    await this.sqlDb.run('UPDATE _cursors SET date = ? WHERE name = ?', [cursor, this.name])
    this.cursor = cursor
  }

  async nextEvents(): Promise<PSHEvent[]> {
    const latest = await this.sqlDb.findOne<PSHRawEvent>('SELECT col, id, type, date, before, after FROM _events WHERE col = ? AND type = ? AND date > ? ORDER BY date ASC LIMIT 1', [this.trigger.col, this.trigger.on, this.cursor])
    if (!latest) {
      return []
    }
    const all = await this.sqlDb.query<PSHRawEvent>('SELECT col, id, type, date, before, after FROM _events WHERE col = ? AND type = ? AND date = ? ORDER BY date ASC', [this.trigger.col, this.trigger.on, latest.date])
    return all.map(inflate)
  }

  async countEvents(): Promise<number> {
    return this.sqlDb.count(`SELECT count(*) FROM _events WHERE date > ${this.cursor}`)
  }

  async start() {
    if (this.stopped) {
      return
    }
    
    await this.fetchCursor()
    console.log('PSHEE.start', this.name, this.cursor)

    let emptyCount = 0
    while (!this.stopped) {
      const count = await this.countEvents()
      if (count > 0) {
        console.log('PSHEE', this.name, count, 'to process')
      }
      const events = uniq(await this.nextEvents(), false, e => e.id)
      if (!isEmpty(events)) {
        console.log('PSHEE', this.name, 'found', events.length, 'events (deduplicated)')
      }

      try {
        if (isEmpty(events)) {
          emptyCount += 1
          const millis = emptyCount > 60 ? 2000 : emptyCount > 10 ? 1000 : 250
          // log('PSHEE.empty: sleep', { millis })
          await sleep(millis)
        } else {
          for (const event of events) {
            if (event) {
              emptyCount = 0
              console.log('PSHEE.event', event.type, event.id, new Date(event.date))
              await this.trigger.call(event)
              if (!this.cursor || event.date > this.cursor) {
                await this.writeCursor(event.date)
              }
            } else {
              console.warn('PSHEE.event null')
            }
          }
        }
      } catch (e) {
        console.error(`PSHEE ${this.name} fail, sleeping`)
        console.error(e)
        sleep(10000)
      }
    }
    console.log('PSHEE.stop', this.name)
  }

  async stop() {
    this.stopped = true
  }
}

type PSHRawEvent = Omit<PSHEvent, 'before'|'after'> & { before: string, after: string }
const inflate = (rawEvent: PSHRawEvent): PSHEvent => {
  const before = rawEvent.before && JSON.parse(rawEvent.before)
  const after = rawEvent.after && JSON.parse(rawEvent.after)
  return { ...rawEvent, before, after }
}