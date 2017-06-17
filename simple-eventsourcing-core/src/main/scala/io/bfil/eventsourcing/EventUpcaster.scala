package io.bfil.eventsourcing

trait EventUpcaster[Event] {
  def upcastEvent(event: Event): Seq[Event]
}
