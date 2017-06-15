package io.bfil.eventsourcing

import scala.concurrent.Future

trait OffsetStore {
  def load(offsetId: String): Future[Long]
  def save(offsetId: String, value: Long): Future[Unit]
}
