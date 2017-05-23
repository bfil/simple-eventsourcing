package io.bfil.eventsourcing

import java.time.Instant

case class EventEnvelope[Event](event: Event, offset: Long, timestamp: Instant = Instant.now)
