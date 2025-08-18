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
  private collectionRunners: Map<string, PSHCollectionRunner> = new Map()
  
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

    await this.initialize()
    maybeLog('PSHEE.register', descriptor, subscriptionId)
    await this.track(trigger.col, trigger.on)
    
    const date = Date.now()
    const sql = 'INSERT INTO _cursors (name, date) VALUES (?, ?)'
    await this.sqlDb.insert<{ date: number }>(sql, [subscriptionId, date])
    let runner = this.collectionRunners.get(trigger.col)
    if (!runner) {
      runner = new PSHCollectionRunner(this.sqlDb, trigger.col)
      this.collectionRunners.set(trigger.col, runner)
    }
    
    // Add trigger to runner
    runner.addTrigger(modifiedTrigger as PSHTrigger, subscriptionId, date)
    
    // Start runner if not already running
    if (!runner.isRunning) {
      maybeLog('PSHEE.register/STARTING runner for', descriptor)
      runner.start()
    } else {
      maybeLog('PSHEE.register/runner already running for', descriptor)
    }
    maybeLog('PSHEE.register/FINISHED', descriptor, subscriptionId)
    
    return () => this.unregister(subscriptionId)
  }

  private async unregister(subscriptionId: string) {
    // Find which collection runner has this subscription
    for (const [col, runner] of this.collectionRunners) {
      if (runner.hasTrigger(subscriptionId)) {
        await runner.removeTrigger(subscriptionId)
        
        // Clean up cursor
        await this.sqlDb.run('DELETE FROM _cursors WHERE name = ?', [subscriptionId])
        
        // If runner has no more triggers, stop and remove it
        if (runner.triggerCount === 0) {
          await runner.stop()
          this.collectionRunners.delete(col)
        }
        
        maybeLog('PSHEE.unregister/SUCCESS', subscriptionId)
        return
      }
    }
  }

  async reset() {
    await this.stop()
    this.collectionRunners.clear()
    this.initialized = false
  }

  async start() {
    await Promise.all(Array.from(this.collectionRunners.values()).map(runner => runner.start()))
  }

  async stop() {
    await Promise.all(Array.from(this.collectionRunners.values()).map(runner => runner.stop()))
  }
}

interface TriggerRegistration {
  trigger: PSHTrigger
  subscriptionId: string
  cursor: number
}

class PSHCollectionRunner {
  private stopped = true
  private triggers: Map<string, TriggerRegistration> = new Map()

  constructor(private sqlDb: PSHSQLiteWrapper, private col: string) {}

  get isRunning() {
    return !this.stopped
  }

  get triggerCount() {
    return this.triggers.size
  }

  get descriptor() { 
    return `${this.col}[${Array.from(this.triggers.values()).map(t => t.trigger.on).join(',')}]`
  }

  hasTrigger(subscriptionId: string): boolean {
    return this.triggers.has(subscriptionId)
  }

  addTrigger(trigger: PSHTrigger, subscriptionId: string, cursor: number) {
    this.triggers.set(subscriptionId, { trigger, subscriptionId, cursor })
  }

  async removeTrigger(subscriptionId: string) {
    this.triggers.delete(subscriptionId)
  }

  private async fetchCursor(subscriptionId: string): Promise<number> {
    const existing = await this.sqlDb.findOne<{ date: number, name: string }>('SELECT * FROM _cursors WHERE name = ?', [subscriptionId])
    if (existing) {
      return existing.date
    } else {
      const date = Date.now()
      await this.sqlDb.insert<{ date: number }>('INSERT INTO _cursors (name, date) VALUES (?, ?) ON CONFLICT (name) DO NOTHING', [subscriptionId, date])
      return date
    }
  }

  private async writeCursor(subscriptionId: string, cursor: number) {
    await this.sqlDb.run('UPDATE _cursors SET date = ? WHERE name = ?', [cursor, subscriptionId])
    
    // Update local cursor
    const registration = this.triggers.get(subscriptionId)
    if (registration) {
      registration.cursor = cursor
    }
  }

  private getEarliestCursor(): number {
    let earliest = Infinity
    for (const registration of this.triggers.values()) {
      if (registration.cursor < earliest) {
        earliest = registration.cursor
      }
    }
    return earliest === Infinity ? Date.now() : earliest
  }

  async nextEvents(): Promise<PSHEvent[]> {
    const earliestCursor = this.getEarliestCursor()
    const latest = await this.sqlDb.findOne<PSHRawEvent>('SELECT col, id, type, date, before, after FROM _events WHERE col = ? AND date > ? ORDER BY date ASC LIMIT 1', [this.col, earliestCursor])
    if (!latest) {
      return []
    }
    const all = await this.sqlDb.query<PSHRawEvent>('SELECT col, id, type, date, before, after FROM _events WHERE col = ? AND date = ? ORDER BY date ASC', [this.col, latest.date])
    return all.map(inflate)
  }

  async countEvents(): Promise<number> {
    const earliestCursor = this.getEarliestCursor()
    const result = await this.sqlDb.findOne<{ count: number }>('SELECT count(*) as count FROM _events WHERE col = ? AND date > ?', [this.col, earliestCursor])
    return result?.count || 0
  }

  async start() {
    this.stopped = false
    // Fetch cursors for all triggers
    for (const [subscriptionId, registration] of this.triggers) {
      registration.cursor = await this.fetchCursor(subscriptionId)
    }
    
    maybeLog('PSHEE.start', this.descriptor, this.getEarliestCursor())

    let emptyCount = 0
    while (!this.stopped && this.triggers.size > 0) {
      const count = await this.countEvents()
      if (count > 0) {
        maybeLog('PSHEE', this.descriptor, count, 'to process. earliest cursor=', this.getEarliestCursor())
      }
      const rawEvents = await this.nextEvents()
      const events = uniq(rawEvents, false, e => e.id)
      if (events.length !== rawEvents.length) {
        maybeWarn(`PSHEE ${this.descriptor} found ${rawEvents.length} events, de-duplicated to ${events.length}`)
      }
      if (isEmpty(events) && !isEmpty(rawEvents)) {
        maybeLog('PSHEE', this.descriptor, 'found no events (deduplicated)')
      }

      try {
        if (isEmpty(events)) {
          emptyCount += 1
          const millis = emptyCount > 60 ? 2000 : emptyCount > 10 ? 1000 : 250
          await sleep(millis)
        } else {
          for (const event of events) {
            if (event) {
              emptyCount = 0
              maybeLog('PSHEE.event', event.type, event.id, new Date(event.date))
              
              // Process event for all applicable triggers
              for (const [subscriptionId, registration] of this.triggers) {
                const { trigger, cursor } = registration
                
                // Only process if this trigger is interested in this event type and cursor is behind
                if (trigger.on === event.type || trigger.on === 'write' && event.date > cursor) {
                  await trigger.call(event)
                  maybeLog('PSHEE.event/advancing cursor', subscriptionId, cursor, '->', event.date)
                  await this.writeCursor(subscriptionId, event.date)
                }
              }
            } else {
              maybeWarn('PSHEE.event null')
            }
          }
        }
      } catch (e) {
        maybeError(`PSHEE ${this.descriptor} fail, sleeping`)
        maybeError(e)
        await sleep(10000)
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