import EventEmitter from "eventemitter3"
import PSHEvent, { PSHEventType } from "./PSHEvent"
import Pea from "../Pea"

export default class PSHEventsLight {
  private emitter: EventEmitter

  constructor() {
    this.emitter = new EventEmitter()
  }

  on<Data extends Pea=Pea, Event extends PSHEvent<Data>=PSHEvent<Data>>(collection: string, event: PSHEventType, listener: (event: Event) => void) {
    this.emitter.on(`${collection}.${event}`, listener)
    return () => { this.emitter.off(`${collection}.${event}`, listener) }
  }

  onDocument<Data extends Pea=Pea, Event extends PSHEvent<Data>=PSHEvent<Data>>(collection: string, id: string, event: Event['type'], listener: (event: Event) => void) {
    this.emitter.on(`${collection}.${id}.${event}`, listener)
    return () => { this.emitter.off(`${collection}.${id}.${event}`, listener) }
  }

  emit(event: PSHEvent) {
    this.emitter.emit(`${event.col}.${event.type}`, event)
    this.emitter.emit(`${event.col}.${event.id}.${event.type}`, event)
  }
}