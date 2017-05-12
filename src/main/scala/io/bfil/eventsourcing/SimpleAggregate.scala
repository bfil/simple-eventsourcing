package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class SimpleAggregate[Event, State](implicit executionContext: ExecutionContext) {
  self: JournalProvider[Event] =>

  val aggregateId: String
  protected val initialState: State

  def onEvent(state: State, event: Event): State

  def recover(): Future[State] =
    for {
      events <- journal.read(aggregateId)
      state = events.foldLeft(initialState)(onEvent)
    } yield state

  def persist(currentState: State, events: Event*): Future[State] =
    for {
      _ <- journal.write(aggregateId, events)
      newState = events.foldLeft(currentState)(onEvent)
    } yield newState

}
