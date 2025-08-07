import Pea from './Pea'
import type PSHCollection from './PSHCollection'
import { PSHPK } from './PSHPK'

export default class PSHRef {
  collection: PSHCollection
  id: PSHPK

  constructor(collection: PSHCollection, id: PSHPK) {
    this.collection = collection
    this.id = id
  }

  async get<T extends Pea>(): Promise<T|null> {
    return this.collection.get(this.id)
  }

  async set(ob: Pea) {
    await this.collection.save(ob, this.id)
  }

  toWrite(ob: Pea) {
    return this.collection.toWrite(this.id, ob)
  }
}