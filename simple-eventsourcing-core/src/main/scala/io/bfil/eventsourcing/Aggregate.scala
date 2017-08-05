package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

abstract class Aggregate[Event, State](journal: JournalWithOptimisticLocking[Event])(implicit executionContext: ExecutionContext) {

  val aggregateId: String
  protected val initialState: State

  def onEvent(state: State, event: Event): State

  protected def retry[T](n: Int)(f: => Future[T]): Future[T] =
    f recoverWith {
      case _: OptimisticLockException if n > 0 => retry(n - 1)(f)
    }

  def recover(): Future[VersionedState[State]] =
    for {
      events <- journal.read(aggregateId)
      versionedState = events.foldLeft(VersionedState(initialState)) { case (versionedState, eventWithOffset) =>
        VersionedState(onEvent(versionedState.state, eventWithOffset.event), eventWithOffset.offset)
      }
    } yield versionedState

  def persist(currentVersionedState: VersionedState[State], events: Event*): Future[State] = {
    for {
      _ <- journal.write(aggregateId, currentVersionedState.version, events)
      newState = events.foldLeft(currentVersionedState.state)(onEvent)
    } yield newState
  }

  protected implicit class MappableFutureState(future: Future[State]) {
    def mapStateTo[T <: State : ClassTag] = future.mapTo[T]
  }

}
