package io.bfil.eventsourcing.inmemory

import scala.collection.mutable
import scala.concurrent.Future

import io.bfil.eventsourcing.{Journal, OptimisticLockException}

class InMemoryJournalWithOptimisticLocking[Event] extends Journal[(Event, Long)] {
  private val eventsByAggregate: mutable.Map[String, Seq[(Event, Long)]] = mutable.Map.empty
  def read(aggregateId: String): Future[Seq[(Event, Long)]] = Future.successful {
    eventsByAggregate.getOrElse(aggregateId, Seq.empty)
  }
  def write(aggregateId: String, events: Seq[(Event, Long)]): Future[Unit] = Future.successful {
    val currentEvents = eventsByAggregate.getOrElse(aggregateId, Seq.empty)
    val firstSequenceNr = events.headOption
                                .map { case (_, sequenceNr) => sequenceNr }
                                .getOrElse(Long.MaxValue)
    if (firstSequenceNr > currentEvents.length) {
      eventsByAggregate += aggregateId -> (currentEvents ++ events)
    } else throw new OptimisticLockException(s"Unexpected sequence number for aggregate '$aggregateId'")
  }
}

