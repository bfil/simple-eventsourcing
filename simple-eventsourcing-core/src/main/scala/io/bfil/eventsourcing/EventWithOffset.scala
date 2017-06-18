package io.bfil.eventsourcing

case class EventWithOffset[Event](event: Event, offset: Long)
