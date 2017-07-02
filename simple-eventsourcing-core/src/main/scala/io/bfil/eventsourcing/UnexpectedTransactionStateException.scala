package io.bfil.eventsourcing

class UnexpectedTransactionStateException[State](
  state: State,
  cause: Throwable = null
) extends RuntimeException(s"Unexpected transaction state: $state", cause)
