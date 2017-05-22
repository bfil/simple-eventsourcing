package io.bfil.eventsourcing

import scala.concurrent.Future

trait EventStream[Event] {
  def subscribe(f: ((Event, Long)) => Future[Unit], offset: Long = 0): Unit
}
