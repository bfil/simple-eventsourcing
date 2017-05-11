package io.bfil.eventsourcing

import scala.concurrent.Future

class InMemoryJournal[Event] extends Journal[Event] {
  private var events: Seq[Event] = Seq.empty
  def read(aggregateId: String): Future[Seq[Event]] = Future.successful(events)
  def write(aggregateId: String, events: Seq[Event]): Future[Unit] = Future.successful(this.events = this.events ++ events)
}
