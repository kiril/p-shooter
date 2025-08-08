import Pea from './Pea'
import PSHEvent, { PSHEventType } from './PSHEvent'

export default interface PSHTrigger<DataType extends Pea=Pea, Event extends PSHEvent<DataType>=PSHEvent<DataType>> {
  col: string
  on: PSHEventType
  call: (event: Event) => (void | Promise<void>)
}

export const cursorName = <DataType extends Pea=Pea, Event extends PSHEvent<DataType>=PSHEvent<DataType>>(trigger: PSHTrigger<DataType,Event>) => `${trigger.col}_${trigger.on}`