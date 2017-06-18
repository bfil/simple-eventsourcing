package io.bfil.eventsourcing.inmemory

import scala.collection.mutable
import scala.concurrent.Future

import io.bfil.eventsourcing.{Snapshot, SnapshotStore}

class InMemorySnapshotStore[State] extends SnapshotStore[State] {
  private val snapshots: mutable.Map[String, Snapshot[State]] = mutable.Map.empty
  def load(aggregateId: String): Future[Option[Snapshot[State]]] = Future.successful {
    snapshots.get(aggregateId)
  }
  def save(aggregateId: String, snapshot: Snapshot[State]): Future[Unit] = Future.successful {
    snapshots += aggregateId -> snapshot
  }
}

