package io.bfil.eventsourcing

class OptimisticLockException(
  message: String,
  cause: Throwable = null
) extends RuntimeException(message, cause)
