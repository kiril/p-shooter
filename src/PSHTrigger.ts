import PSHEvent, { PSHEventType } from './PSHEvent'

export default interface PSHTrigger {
  col: string
  on: PSHEventType
  call: (event: PSHEvent) => (void | Promise<void>)
}

export const cursorName = (trigger: PSHTrigger) => `${trigger.col}_${trigger.on}`