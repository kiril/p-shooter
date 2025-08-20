import Pea from './Pea'
import type PSHCollection from './PSHCollection'

export default class PSHRef {
  collection: PSHCollection
  id: string

  constructor(collection: PSHCollection, id: string) {
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