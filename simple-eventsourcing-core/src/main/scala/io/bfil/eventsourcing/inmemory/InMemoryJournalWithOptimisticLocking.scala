package io.bfil.eventsourcing.inmemory

import scala.collection.mutable
import scala.concurrent.Future

import io.bfil.eventsourcing.{EventWithOffset, OptimisticLockException, JournalWithOptimisticLocking}

class InMemoryJournalWithOptimisticLocking[Event] extends JournalWithOptimisticLocking[Event] {
  private val eventsByAggregate: mutable.Map[String, Seq[EventWithOffset[Event]]] = mutable.Map.empty
  def read(aggregateId: String, offset: Long = 0): Future[Seq[EventWithOffset[Event]]] = Future.successful {
    eventsByAggregate.getOrElse(aggregateId, Seq.empty).filter(_.offset > offset)
  }
  def write(aggregateId: String, lastSeenOffset: Long, events: Seq[Event]): Future[Unit] = Future.successful {
    val currentEvents = eventsByAggregate.getOrElse(aggregateId, Seq.empty)
    val eventsWithOffset = events.zipWithIndex.map {
      case (event, i) => EventWithOffset(event, lastSeenOffset + 1 + i)
    }
    if (lastSeenOffset < currentEvents.length) {
      throw new OptimisticLockException(s"Unexpected offset ($lastSeenOffset) for aggregate '$aggregateId'")
    } else {
      eventsByAggregate += aggregateId -> (currentEvents ++ eventsWithOffset)
    }
  }
}
