package io.bfil.eventsourcing

import scala.concurrent.Future

trait EventStream[Event] {
  def subscribe(f: EventEnvelope[Event] => Future[Unit], offset: Long = 0): Unit
}
