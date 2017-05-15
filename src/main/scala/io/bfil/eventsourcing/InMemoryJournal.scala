package io.bfil.eventsourcing

import scala.collection.mutable
import scala.concurrent.Future

class InMemoryJournal[Event] extends Journal[Event] {
  private val eventsByAggregate: mutable.Map[String, Seq[Event]] = mutable.Map.empty
  def read(aggregateId: String): Future[Seq[Event]] = Future.successful {
    eventsByAggregate.get(aggregateId).getOrElse(Seq.empty)
  }
  def write(aggregateId: String, events: Seq[Event]): Future[Unit] = Future.successful {
    val currentEvents = eventsByAggregate.get(aggregateId).getOrElse(Seq.empty)
    eventsByAggregate += aggregateId -> (currentEvents ++ events)
  }
}
