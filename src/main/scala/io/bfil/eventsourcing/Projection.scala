package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class Projection[Event](implicit executionContext: ExecutionContext) {
  self: EventStreamProvider[Event] =>

  def processEvent(f: Event): Future[Unit]

  def run() = eventStream.subscribe(processEvent)

}
