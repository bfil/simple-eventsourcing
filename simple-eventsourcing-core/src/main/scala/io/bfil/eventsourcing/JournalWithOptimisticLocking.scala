package io.bfil.eventsourcing

import scala.concurrent.Future

trait JournalWithOptimisticLocking[Event] {
  def read(aggregateId: String, offset: Long = 0): Future[Seq[EventWithOffset[Event]]]
  def write(aggregateId: String, lastSeenOffset: Long, events: Seq[Event]): Future[Unit]
}
