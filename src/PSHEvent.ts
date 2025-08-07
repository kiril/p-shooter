import Pea from './Pea'
import { PSHPK } from './PSHPK'

export type PSHEventType = 'insert'|'update'|'write'|'delete'

export default interface PSHEvent<Data extends Pea=Pea> {
  col: string
  id: PSHPK
  type: PSHEventType
  date: number
  before?: Data
  after?: Data
}

export interface PSHWrite<Data extends Pea=Pea> extends PSHEvent<Data> {
  before?: Data
  after: Data
}

export interface PSHUpdate<Data extends Pea=Pea> extends PSHEvent<Data> {
  before: Data
  after: Data
}

export interface PSHDelete<Data extends Pea=Pea> extends PSHEvent<Data> {
  before: Data
}

export interface PSHEventBatch {
  events: PSHEvent[]
}