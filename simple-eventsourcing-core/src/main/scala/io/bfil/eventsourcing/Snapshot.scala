package io.bfil.eventsourcing

case class Snapshot[State](state: State, offset: Long = 0)
