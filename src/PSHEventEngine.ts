import { isEmpty, uniq } from 'underscore'
import uuid from 'react-native-uuid'

import PSHEvent, { PSHEventType } from './PSHEvent'
import PSHSQLiteWrapper from './PSHSQLiteWrapper'
import PSHTrigger, { cursorName } from './PSHTrigger'
import { sleep } from './util'
import Pea from './Pea'
import { maybeLog, maybeError, maybeWarn } from './shared'


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
    maybeLog('PSHEventEngine.initialize/SUCCESS')
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

  async register<DataType extends Pea=Pea, Event extends PSHEvent<DataType>=PSHEvent<DataType>>(trigger: PSHTrigger<DataType,Event>): Promise<() => void> {
    const subscriptionId = uuid.v4() as string
    const modifiedTrigger = { ...trigger } // copy to avoid mutating original
    const descriptor = `${trigger.col}.${trigger.on}`
    
    const promise = new Promise<void>((resolve, reject) => {
      this.initialize()
        .then(() => {
          maybeLog('PSHEE.register/NEW', descriptor, subscriptionId)
          return this.track(trigger.col, trigger.on)
        })
        .then(() => {
          const date = Date.now()
          const sql = 'INSERT INTO _cursors (name, date) VALUES (?, ?)'
          return this.sqlDb.insert<{ date: number }>(sql, [subscriptionId, date])
            .then(() => date)
        })
        .then(date => {
          const runner = new PSHTriggerRunner(this.sqlDb, modifiedTrigger as PSHTrigger, date, subscriptionId)
          this.triggerRunners.push(runner)
          return runner.start()
        })
        .then(resolve)
        .catch(e => {
          maybeError('PSHEE.register/ERROR', e)
          reject(e)
        })
    })

    await promise
    
    return () => this.unregister(subscriptionId)
  }

  private async unregister(subscriptionId: string) {
    const runnerIndex = this.triggerRunners.findIndex(r => r.subscriptionId === subscriptionId)
    if (runnerIndex >= 0) {
      const runner = this.triggerRunners[runnerIndex]
      await runner.stop()
      this.triggerRunners.splice(runnerIndex, 1)
      
      // Clean up cursor
      await this.sqlDb.run('DELETE FROM _cursors WHERE name = ?', [subscriptionId])
      
      maybeLog('PSHEE.unregister/SUCCESS', subscriptionId)
    }
  }



  async reset() {
    await this.stop()
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
  private stopped = false

  constructor(private sqlDb: PSHSQLiteWrapper, private trigger: PSHTrigger, private cursor: number, public readonly subscriptionId: string) {}

  get isRunning() {
    return !this.stopped
  }

  get descriptor() { return `${this.trigger.col}.${this.trigger.on}` }

  private async fetchCursor() {
    if (this.cursor === null) {
      let date: number
      const existing = await this.sqlDb.findOne<{ date: number, name: string }>('SELECT * FROM _cursors WHERE name = ?', [this.subscriptionId])
      if (existing) {
        date = existing.date
      } else {
        date = Date.now()
        await this.sqlDb.insert<{ date: number }>('INSERT INTO _cursors (name, date) VALUES (?, ?) ON CONFLICT (name) DO NOTHING', [this.subscriptionId, date])
      }
      this.cursor = date
    }
  }

  private async writeCursor(cursor: number) {
    await this.sqlDb.run('UPDATE _cursors SET date = ? WHERE name = ?', [cursor, this.subscriptionId])
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
    maybeLog('PSHEE.start', this.subscriptionId, this.cursor)

    let emptyCount = 0
    while (!this.stopped) {
      const count = await this.countEvents()
      if (count > 0) {
        maybeLog('PSHEE', this.descriptor, count, 'to process. cursor=', this.cursor)
      }
      const rawEvents = await this.nextEvents()
      const events = uniq(rawEvents, false, e => e.id)
      if (events.length !== rawEvents.length) {
        maybeWarn(`PSHEE ${this.descriptor} found ${rawEvents.length} events, de-duplicated to ${events.length}`)
        maybeLog(rawEvents)
      }
      if (isEmpty(events) && !isEmpty(rawEvents)) {
        maybeLog('PSHEE', this.descriptor, 'found no events (deduplicated)')
      }
      if (!isEmpty(events) && events.length !== rawEvents.length) {
        maybeLog('PSHEE', this.descriptor, 'found', events.length, 'events (deduplicated from', rawEvents.length, ')')
      }

      try {
        if (isEmpty(events)) {
          emptyCount += 1
          const millis = emptyCount > 60 ? 2000 : emptyCount > 10 ? 1000 : 250
          maybeLog('PSHEE.empty: sleep', { millis })
          await sleep(millis)
        } else {
          for (const event of events) {
            if (event) {
              emptyCount = 0
              maybeLog('PSHEE.event', event.type, event.id, new Date(event.date))
              await this.trigger.call(event)
              if (!this.cursor || event.date > this.cursor) {
                maybeLog('PSHEE.event/advancing cursor', this.cursor, '->', event.date)
                await this.writeCursor(event.date)
              }
            } else {
              maybeWarn('PSHEE.event null')
            }
          }
        }
      } catch (e) {
        maybeError(`PSHEE ${this.descriptor} fail, sleeping`)
        maybeError(e)
        sleep(10000)
      }
    }
    maybeLog('PSHEE.stop', this.descriptor)
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