package io.bfil.eventsourcing

import scala.collection.mutable
import scala.concurrent.Future

class InMemoryOffsetStore extends OffsetStore {
  private var offsets: mutable.Map[String, Long] = mutable.Map.empty
  def load(offsetId: String): Future[Long] = Future.successful {
    offsets.get(offsetId).getOrElse(0L)
  }
  def save(offsetId: String, value: Long): Future[Unit] = Future.successful {
    offsets += offsetId -> value
  }
}
