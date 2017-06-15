package io.bfil.eventsourcing

import scala.concurrent.Future

trait Journal[Event] {
  def read(aggregateId: String): Future[Seq[Event]]
  def write(aggregateId: String, events: Seq[Event]): Future[Unit]
}
