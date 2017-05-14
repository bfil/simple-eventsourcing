package io.bfil.eventsourcing

import scala.concurrent.Future

trait OffsetStore {
  def read(offsetId: String): Future[Long]
  def write(offsetId: String, value: Long): Future[Unit]
}

trait OffsetStoreProvider {
  def offsetStore: OffsetStore
}
