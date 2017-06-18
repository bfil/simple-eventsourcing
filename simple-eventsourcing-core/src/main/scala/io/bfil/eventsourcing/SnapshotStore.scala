package io.bfil.eventsourcing

import scala.concurrent.Future

trait SnapshotStore[State] {
  def load(aggregateId: String): Future[Option[Snapshot[State]]]
  def save(aggregateId: String, snapshot: Snapshot[State]): Future[Unit]
}