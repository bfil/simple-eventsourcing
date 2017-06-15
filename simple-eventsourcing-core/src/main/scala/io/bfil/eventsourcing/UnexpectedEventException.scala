package io.bfil.eventsourcing

class UnexpectedEventException[Event, State](
  event: Event,
  state: State,
  cause: Throwable = null
) extends RuntimeException(s"Unexpected event $event in state $state", cause)
