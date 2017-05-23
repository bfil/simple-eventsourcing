package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class Projection[Event](eventStream: EventStream[Event])(implicit executionContext: ExecutionContext) {

  def processEvent(f: Event): Future[Unit]

  def run() = eventStream subscribe processEvent

}
