package io.bfil.eventsourcing

import java.util.concurrent.Executors

import scala.concurrent.ExecutionContext

trait SingleThreadedExecutionContext {
  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
}
