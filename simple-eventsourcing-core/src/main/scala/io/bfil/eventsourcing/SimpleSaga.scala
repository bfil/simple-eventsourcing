package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class SimpleSaga[TransactionState, TransactionResult](store: TransactionStore[TransactionState])(implicit executionContext: ExecutionContext) {

  store.loadIncompleteTransactions() foreach { transfers =>
    transfers foreach continueTransaction
  }

  protected def performNextAction(state: TransactionState): Future[Option[TransactionState]]

  protected def completeTransaction(state: TransactionState): TransactionResult

  protected def startTransaction(initialState: TransactionState): Future[TransactionResult] =
    saveStateAndContinueTransaction(None, Some(initialState))

  private def saveStateAndContinueTransaction(currentState: Option[TransactionState], newState: Option[TransactionState]): Future[TransactionResult] =
    newState match {
      case Some(state) => store.saveTransactionState(state) flatMap { _ =>
                            continueTransaction(state)
                          }
      case None        => currentState match {
                            case Some(state) => Future.successful(completeTransaction(state))
                            case None        => throw new RuntimeException("Unable to complete transaction with no state")
                          }
    }

  private def continueTransaction(state: TransactionState): Future[TransactionResult] =
    for {
      newState  <- performNextAction(state)
      result    <- saveStateAndContinueTransaction(Some(state), newState)
    } yield result
}
