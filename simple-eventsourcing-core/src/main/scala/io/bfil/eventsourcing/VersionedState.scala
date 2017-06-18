package io.bfil.eventsourcing

case class VersionedState[State](state: State, version: Long = 0)
