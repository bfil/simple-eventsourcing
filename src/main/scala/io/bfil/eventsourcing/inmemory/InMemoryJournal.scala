package io.bfil.eventsourcing.inmemory

import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.concurrent.Future

import io.bfil.eventsourcing.Journal

class InMemoryJournal[Event] extends Journal[Event] {
  private val offset = new AtomicLong(0)
  private val eventsByAggregate: mutable.Map[String, Seq[Event]] = mutable.Map.empty
  def read(aggregateId: String): Future[Seq[Event]] = Future.successful {
    eventsByAggregate.get(aggregateId).getOrElse(Seq.empty)
  }
  def write(aggregateId: String, events: Seq[Event]): Future[Long] = Future.successful {
    val currentEvents = eventsByAggregate.get(aggregateId).getOrElse(Seq.empty)
    eventsByAggregate += aggregateId -> (currentEvents ++ events)
    offset.incrementAndGet()
  }
}
