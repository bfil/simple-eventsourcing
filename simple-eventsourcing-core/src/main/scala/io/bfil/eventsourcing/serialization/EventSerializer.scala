package io.bfil.eventsourcing.serialization

trait EventSerializer[Event] {
  def serialize(event: Event): SerializedEvent[Event]
  def deserialize(manifest: String, data: String): Event
}
