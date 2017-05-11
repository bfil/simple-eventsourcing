package io.bfil.eventsourcing

import scala.concurrent.Future

trait Cache[State] {
  def get(aggregateId: String): Future[Option[State]]
  def put(aggregateId: String, oldState: Option[State], newState: State): Future[Unit]
  def remove(aggregateId: String): Future[Unit]
}

trait CacheProvider[State] {
  val cache: Cache[State]
}
