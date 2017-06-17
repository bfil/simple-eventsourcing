package io.bfil.eventsourcing.inmemory

import scala.collection.mutable
import scala.concurrent.Future

import io.bfil.eventsourcing.Journal

class InMemoryJournal[Event] extends Journal[Event] {
  private val eventsByAggregate: mutable.Map[String, Seq[Event]] = mutable.Map.empty
  def read(aggregateId: String): Future[Seq[Event]] = Future.successful {
    eventsByAggregate.getOrElse(aggregateId, Seq.empty)
  }
  def write(aggregateId: String, events: Seq[Event]): Future[Unit] = Future.successful {
    val currentEvents = eventsByAggregate.getOrElse(aggregateId, Seq.empty)
    eventsByAggregate += aggregateId -> (currentEvents ++ events)
  }
}
