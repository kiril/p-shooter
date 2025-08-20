import Pea from '../Pea'

export type PSHEventType = 'write'|'delete'

export default interface PSHEvent<Data extends Pea=Pea> {
  col: string
  id: string
  type: PSHEventType
  date: number
  data?: Data
}

export interface PSHWrite<Data extends Pea=Pea> extends PSHEvent<Data> {
  type: 'write'
  data: Data
}

export interface PSHDelete<Data extends Pea=Pea> extends Omit<PSHEvent<Data>, 'data'> {
  type: 'delete'
}

export interface PSHEventBatch {
  events: PSHEvent[]
}