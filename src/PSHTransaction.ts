import { SQLTransaction } from 'react-native-sqlite-2'

import PSHRef from './PSHRef'
import PSHCollection from './PSHCollection'
import PSHDeferredWrite from './PSHDeferredWrite'
import PSHSQLiteWrapper from './PSHSQLiteWrapper'
import Pea from './Pea'


export default class PSHTransaction {
  sqlDb: PSHSQLiteWrapper
  toAdd: PSHAdd[] = []

  constructor(sqlDb: PSHSQLiteWrapper) {
    this.sqlDb = sqlDb
  }

  get count() {
    return this.toAdd.length
  }

  add<T extends Pea>(col: PSHCollection, doc: T) {
    col.initialize()
    this.toAdd.push(new PSHAdd(col, doc))
  }

  async executeBatch<T>(items: Iterable<T>, callback: (x: T) => void, batchSize: number) {
    let count = 0
    for (const item of items) {
      callback(item)
      if (this.toAdd.length >= batchSize) {
        count += this.toAdd.length
        await this.execute()
      }
    }
    if (this.toAdd.length > 0) {
      count += this.toAdd.length
      await this.execute()
    }
    return count
  }

  async executeAsyncBatch<T>(items: AsyncGenerator<T>, callback: (x: T) => Promise<void>, batchSize: number) {
    let count = 0
    for await (const item of items) {
      await callback(item)
      if (this.toAdd.length >= batchSize) {
        count += this.toAdd.length
        await this.execute()
      }
    }
    if (this.toAdd.length > 0) {
      count += this.toAdd.length
      await this.execute()
    }
    return count
  }

  async execute() {
    console.log('PSHTransaction.execute...')
    const writeAll = (tx: SQLTransaction) => this.toAdd.map(a => a.toWrite()).forEach(({ sql, args }) => tx.executeSql(sql, args))
    await this.sqlDb.transaction(writeAll).then(() => this.toAdd = [])
  }
}

interface PSHCommand {
  toWrite: () => PSHDeferredWrite
}

class PSHAdd implements PSHCommand {
  ref: PSHRef
  obj: Pea

  constructor(col: PSHCollection, obj: Pea) {
    this.ref = col.ref()
    this.obj = obj
  }

  toWrite() {
    return this.ref.toWrite(this.obj)
  }
}