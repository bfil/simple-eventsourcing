package io.bfil.eventsourcing

import java.time.Instant

case class EventEnvelope[Event](
  offset: Long,
  aggregateId: String,
  event: Event,
  timestamp: Instant = Instant.now)
