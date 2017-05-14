package io.bfil.eventsourcing

import scala.concurrent.Future

trait EventStream[Event] {
  def subscribe(f: Event => Future[Unit], offset: Long = 0): Unit
}

trait EventStreamProvider[Event] {
  def eventStream: EventStream[Event]
}
