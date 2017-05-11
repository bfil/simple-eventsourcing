package io.bfil.eventsourcing

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.control.NonFatal

abstract class Aggregate[Event, State <: AggregateState[Event, State]](implicit executionContext: ExecutionContext) {
  self: JournalProvider[Event] with CacheProvider[State] =>

  val aggregateId: String
  protected val initialState: State

  def state(): Future[State] = recover

  private def computeState(currentState: State, events: Seq[Event]): State =
    events.foldLeft(currentState)(onEvent)

  protected def retry[T](n: Int)(f: => Future[T]): Future[T] =
    f recoverWith {
      case ex: OptimisticLockException if n > 0 => retry(n - 1)(f)
    }

  protected def recover(): Future[State] =
    for {
      cachedState <- cache.get(aggregateId)
      state <- cachedState match {
        case Some(cachedState) => Future.successful(cachedState)
        case None =>
          for {
            events <- journal.read(aggregateId)
            journalState = computeState(initialState, events)
            _ <- cache.put(aggregateId, None, journalState)
          } yield journalState
      }
    } yield state

  protected def persist(currentState: State, events: Event*): Future[State] = {
    val newState = computeState(currentState, events)
    for {
      _ <- cache.put(aggregateId, Some(currentState), newState)
      _ <- journal.write(aggregateId, events) recoverWith {
        case NonFatal(ex) => invalidateCachedStateAndFailWith(ex)
      }
    } yield newState
  }

  protected def onEvent(state: State, event: Event): State =
    (state.eventHandler orElse unexpectedEventHandler(state))(event)

  private def invalidateCachedStateAndFailWith(ex: Throwable) =
    cache.remove(aggregateId)
         .flatMap(_ => Future.failed(ex))
         .recoverWith { case NonFatal(_) => Future.failed(ex) }

  protected def unexpectedEventHandler(state: State): State#EventHandler = {
    case event => throw new Exception(s"Unexpected event $event in state $state")
  }

  protected implicit class MappableFutureState(future: Future[State]) {
    def mapStateTo[T <: State : ClassTag] = future.mapTo[T]
  }
}
