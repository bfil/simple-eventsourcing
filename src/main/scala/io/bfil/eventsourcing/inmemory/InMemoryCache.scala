package io.bfil.eventsourcing.inmemory

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.concurrent.Future

import io.bfil.eventsourcing.{Cache, OptimisticLockException}

class InMemoryCache[State] extends Cache[State] {
  private val cache = new ConcurrentHashMap[String, State]().asScala
  def get(aggregateId: String): Future[Option[State]] = Future.successful(cache.get(aggregateId))
  def put(aggregateId: String, oldState: Option[State], newState: State): Future[Unit] = oldState match {
    case Some(oldState) =>
      cache.replace(aggregateId, oldState, newState) match {
        case false => Future.failed(new OptimisticLockException(s"Unexpected cached state for aggregate '$aggregateId'"))
        case true => Future.successful(())
      }
    case None =>
      cache.putIfAbsent(aggregateId, newState) match {
        case Some(oldState) => Future.failed(new OptimisticLockException(s"Unexpected cached state for aggregate '$aggregateId'"))
        case None => Future.successful(())
      }
  }
  def remove(aggregateId: String) = Future.successful(cache.remove(aggregateId))
}
