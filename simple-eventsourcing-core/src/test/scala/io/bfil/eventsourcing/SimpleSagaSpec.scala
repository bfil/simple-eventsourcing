package io.bfil.eventsourcing

import java.util.UUID

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class SimpleSagaSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  val transactionStore = new TransactionStore[BankTransfer] {
    def saveTransactionState(state: BankTransfer) = Future.successful(())
    def loadIncompleteTransactions() = Future.successful(Seq.empty)
  }

  val openBankAccount = new BankAccount {
    def withdraw(transferId: UUID, amount: Int): Future[Unit] = Future.successful(())
    def deposit(transferId: UUID, amount: Int): Future[Unit]  = Future.successful(())
  }
  val closedBankAccount = new BankAccount {
    def withdraw(transferId: UUID, amount: Int): Future[Unit] = Future.failed(new Exception("Unable to withdraw money from a closed account"))
    def deposit(transferId: UUID, amount: Int): Future[Unit]  = Future.failed(new Exception("Unable to deposit money to a closed account"))
  }
  val bankAccountResolver = new BankAccountResolver {
    override def resolve(id: Long): BankAccount = id match {
      case 1 | 2 => openBankAccount
      case _     => closedBankAccount
    }
  }

  val saga = new BankTransferSaga(transactionStore, bankAccountResolver)

  "SimpleSaga" should {

    "perform a bank transfer transaction" in {

      saga.transfer(1, 2, 100).futureValue shouldBe Completed

    }

    "perform a failing bank transfer transaction" in {

      saga.transfer(1, 3, 100).futureValue shouldBe Cancelled

    }

  }

  trait BankAccountResolver {
    def resolve(id: Long): BankAccount
  }

  trait BankAccount {
    def withdraw(transferId: UUID, amount: Int): Future[Unit]
    def deposit(transferId: UUID, amount: Int): Future[Unit]
  }

  case class BankTransfer(id: UUID, status: BankTransferStatus, sourceAccountId: Long, targetAccountId: Long, amount: Int)

  sealed trait BankTransferStatus
  case object WithdrawalPending extends BankTransferStatus
  case object DepositPending extends BankTransferStatus
  case object RefundPending extends BankTransferStatus
  case object Completed extends BankTransferStatus
  case object Cancelled extends BankTransferStatus

  class BankTransferSaga(store: TransactionStore[BankTransfer], accountResolver: BankAccountResolver)
    extends SimpleSaga[BankTransfer, BankTransferStatus](store) {

    def transfer(sourceAccountId: Long, targetAccountId: Long, amount: Int): Future[BankTransferStatus] =
      startTransaction(BankTransfer(
        id              = UUID.randomUUID(),
        status          = WithdrawalPending,
        sourceAccountId = sourceAccountId,
        targetAccountId = targetAccountId,
        amount          = amount
      ))

    protected def performNextAction(transfer: BankTransfer) = transfer.status match {
      case WithdrawalPending =>
        accountResolver.resolve(transfer.sourceAccountId).withdraw(transfer.id, transfer.amount) transform {
          case Success(_) => Success(Some(transfer.copy(status = DepositPending)))
          case Failure(_) => Success(Some(transfer.copy(status = Cancelled)))
        }
      case DepositPending =>
        accountResolver.resolve(transfer.targetAccountId).deposit(transfer.id, transfer.amount) transform {
          case Success(_) => Success(Some(transfer.copy(status = Completed)))
          case Failure(_) => Success(Some(transfer.copy(status = RefundPending)))
        }
      case RefundPending =>
        accountResolver.resolve(transfer.sourceAccountId).deposit(transfer.id, transfer.amount) transform {
          case Success(_) => Success(Some(transfer.copy(status = Cancelled)))
          case Failure(_) => Failure(new Exception("Unable to refund withdrawn money, we need a human here"))
        }
      case _ => Future.successful(None)
    }

    protected def completeTransaction(transfer: BankTransfer) = transfer.status

  }

}