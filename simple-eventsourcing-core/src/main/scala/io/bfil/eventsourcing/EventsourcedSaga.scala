package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class EventsourcedSaga[Event, State <: AggregateState[Event, State], Result](
  journal: Journal[Event]
  )(implicit executionContext: ExecutionContext) extends SimpleAggregate[Event, State](journal) {

  protected val performNextAction: PartialFunction[State, Future[Option[Event]]]

  protected val completeTransaction: PartialFunction[State, Result]

  protected def startTransaction(initialEvent: Event): Future[Result] =
    for {
      state  <- recover
      event   = if(state == initialState) initialEvent
                else                      throw new UnexpectedTransactionStateException(state)
      result <- persistEventAndContinueTransaction(state, Some(event))
    } yield result

  protected def resumeTransaction(): Future[Result] =
    for {
      transfer <- recover
      result   <- continueTransaction(transfer)
    } yield result

  private def persistEventAndContinueTransaction(state: State, event: Option[Event]): Future[Result] =
    event match {
      case Some(event) => persist(state, event) flatMap continueTransaction
      case None        => Future.successful(completeTransaction(state))
    }

  private def continueTransaction(state: State): Future[Result] =
    for {
      event  <- (performNextAction orElse unexpectedTransactionState)(state)
      result <- persistEventAndContinueTransaction(state, event)
    } yield result

  def onEvent(state: State, event: Event): State =
    (state.eventHandler orElse unexpectedEventHandler(state))(event)

  protected def unexpectedEventHandler(state: State): State#EventHandler = {
    case event => throw new UnexpectedEventException(event, state)
  }

  protected val unexpectedTransactionState: PartialFunction[State, Future[Option[Event]]] = {
    case state => Future.failed(new UnexpectedTransactionStateException(state))
  }

}
