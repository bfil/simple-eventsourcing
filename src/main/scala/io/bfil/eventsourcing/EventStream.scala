package io.bfil.eventsourcing

import scala.concurrent.Future

trait EventStream[Event] {
  def subscribe(f: Event => Future[Unit]): Unit
}

trait EventStreamProvider[Event] {
  def eventStream: EventStream[Event]
}
