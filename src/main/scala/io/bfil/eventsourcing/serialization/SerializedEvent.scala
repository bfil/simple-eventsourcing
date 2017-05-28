package io.bfil.eventsourcing.serialization

case class SerializedEvent[Event](manifest: String, data: String)
