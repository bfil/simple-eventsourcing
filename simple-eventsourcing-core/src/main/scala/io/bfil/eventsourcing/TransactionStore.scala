package io.bfil.eventsourcing

import scala.concurrent.Future

trait TransactionStore[TransactionState] {
  def saveTransactionState(state: TransactionState): Future[Unit]
  def loadIncompleteTransactions(): Future[Seq[TransactionState]]
}
