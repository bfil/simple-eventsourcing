package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class Aggregate[Event, State](journal: Journal[(Event, Long)])(implicit executionContext: ExecutionContext) {

  val aggregateId: String
  protected val initialState: State

  def onEvent(state: State, event: Event): State

  protected def retry[T](n: Int)(f: => Future[T]): Future[T] =
    f recoverWith {
      case ex: OptimisticLockException if n > 0 => retry(n - 1)(f)
    }

  def recover(): Future[(State, Long)] =
    for {
      events <- journal.read(aggregateId)
      (state, lastSequenceNr) = events.foldLeft((initialState, 0L)) {
        case ((state, _), (event, sequenceNr)) => (onEvent(state, event), sequenceNr)
      }
    } yield (state, lastSequenceNr)

  def persist(currentState: State, lastSequenceNr: Long, events: Event*): Future[State] = {
    val eventsWithSequenceNumbers = events.zipWithIndex.map { case (event, i) =>
      (event, lastSequenceNr + i + 1)
    }
    for {
      _ <- journal.write(aggregateId, eventsWithSequenceNumbers)
      newState = events.foldLeft(currentState)(onEvent)
    } yield newState
  }

}
