package io.bfil.eventsourcing

import java.util.UUID

import io.bfil.eventsourcing.inmemory.InMemoryJournal
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.util.{Failure, Success}

class EventsourcedSagaSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  val openBankAccount = new BankAccount {
    def withdraw(transferId: UUID, amount: Int): Future[Unit] = Future.successful(())
    def deposit(transferId: UUID, amount: Int): Future[Unit]  = Future.successful(())
  }
  val closedBankAccount = new BankAccount {
    def withdraw(transferId: UUID, amount: Int): Future[Unit] = Future.failed(new Exception("Unable to withdraw money from a closed account"))
    def deposit(transferId: UUID, amount: Int): Future[Unit]  = Future.failed(new Exception("Unable to deposit money to a closed account"))
  }

  trait Context {
    val bankAccountResolver = new BankAccountResolver {
      override def resolve(id: Long): BankAccount = id match {
        case 1 | 2 => openBankAccount
        case _     => closedBankAccount
      }
    }
    val journal = new InMemoryJournal[BankTransferEvent]
    val saga = new BankTransferSaga(UUID.randomUUID(), bankAccountResolver, journal)
  }

  "SagaAggregateSpec" should {

    "perform a bank transfer transaction" in new Context {

      saga.transfer(1, 2, 100).futureValue shouldBe Completed
      journal.read(saga.aggregateId).futureValue shouldBe Seq(
        BankTransferInitialized(saga.id, 1, 2, 100),
        MoneyWithdrawnFromSourceAccount(saga.id, 1, 100),
        MoneyDepositedToTargetAccount(saga.id, 2, 100)
      )

    }

    "perform a failing bank transfer transaction" in new Context {

      saga.transfer(1, 3, 100).futureValue shouldBe Cancelled
      journal.read(saga.aggregateId).futureValue shouldBe Seq(
        BankTransferInitialized(saga.id, 1, 3, 100),
        MoneyWithdrawnFromSourceAccount(saga.id, 1, 100),
        MoneyDepositToTargetAccountFailed(saga.id, 3, 100)
      )

    }

  }

  trait BankAccountResolver {
    def resolve(id: Long): BankAccount
  }

  trait BankAccount {
    def withdraw(transferId: UUID, amount: Int): Future[Unit]
    def deposit(transferId: UUID, amount: Int): Future[Unit]
  }

  sealed trait BankTransferState extends AggregateState[BankTransferEvent, BankTransferState]
  case object Empty extends BankTransferState {
    val eventHandler = EventHandler {
      case BankTransferInitialized(id, sourceAccountId, targetAccountId, amount) =>
        BankTransfer(id, WithdrawalPending, sourceAccountId, targetAccountId, amount)
    }
  }
  case class BankTransfer(id: UUID, status: BankTransferStatus, sourceAccountId: Long, targetAccountId: Long, amount: Int) extends BankTransferState {
    val eventHandler = EventHandler {
      case _: MoneyWithdrawnFromSourceAccount        => copy(status = DepositPending)
      case _: MoneyWithdrawalFromSourceAccountFailed => copy(status = Cancelled)
      case _: MoneyDepositedToTargetAccount          => copy(status = Completed)
      case _: MoneyDepositToTargetAccountFailed      => copy(status = Cancelled)
      case _: MoneyRefundedToSourceAccount           => copy(status = Cancelled)
    }
  }

  sealed trait BankTransferStatus
  case object WithdrawalPending extends BankTransferStatus
  case object DepositPending extends BankTransferStatus
  case object RefundPending extends BankTransferStatus
  case object Completed extends BankTransferStatus
  case object Cancelled extends BankTransferStatus

  sealed trait BankTransferEvent
  case class BankTransferInitialized(id: UUID, sourceAccountId: Long, targetAccountId: Long, amount: Int) extends BankTransferEvent
  case class MoneyWithdrawnFromSourceAccount(id: UUID, sourceAccountId: Long, amount: Int) extends BankTransferEvent
  case class MoneyWithdrawalFromSourceAccountFailed(id: UUID, sourceAccountId: Long, amount: Int) extends BankTransferEvent
  case class MoneyDepositedToTargetAccount(id: UUID, targetAccountId: Long, amount: Int) extends BankTransferEvent
  case class MoneyDepositToTargetAccountFailed(id: UUID, targetAccountId: Long, amount: Int) extends BankTransferEvent
  case class MoneyRefundedToSourceAccount(id: UUID, sourceAccountId: Long, amount: Int) extends BankTransferEvent

  class BankTransferSaga(val id: UUID, accountResolver: BankAccountResolver, journal: Journal[BankTransferEvent])
    extends EventsourcedSaga[BankTransferEvent, BankTransferState, BankTransferStatus](journal) {

    val aggregateId = s"bank-transfer-$id"
    val initialState = Empty

    def transfer(sourceAccountId: Long, targetAccountId: Long, amount: Int) = startTransaction {
      BankTransferInitialized(id, sourceAccountId, targetAccountId, amount)
    }

    def resumeTransfer() = resumeTransaction()

    protected val performNextAction = {
      case transfer: BankTransfer =>
        transfer.status match {
          case WithdrawalPending =>
            accountResolver.resolve(transfer.sourceAccountId).withdraw(transfer.id, transfer.amount) transform {
              case Success(_) => Success(Some(MoneyWithdrawnFromSourceAccount(transfer.id, transfer.sourceAccountId, transfer.amount)))
              case Failure(_) => Success(Some(MoneyWithdrawalFromSourceAccountFailed(transfer.id, transfer.sourceAccountId, transfer.amount)))
            }
          case DepositPending =>
            accountResolver.resolve(transfer.targetAccountId).deposit(transfer.id, transfer.amount) transform {
              case Success(_) => Success(Some(MoneyDepositedToTargetAccount(transfer.id, transfer.targetAccountId, transfer.amount)))
              case Failure(_) => Success(Some(MoneyDepositToTargetAccountFailed(transfer.id, transfer.targetAccountId, transfer.amount)))
            }
          case RefundPending =>
            accountResolver.resolve(transfer.sourceAccountId).deposit(transfer.id, transfer.amount) transform {
              case Success(_) => Success(Some(MoneyRefundedToSourceAccount(transfer.id, transfer.sourceAccountId, transfer.amount)))
              case Failure(_) => Failure(new Exception("Unable to refund withdrawn money, we need a human here"))
            }
          case _ => Future.successful(None)
        }
    }

    protected val completeTransaction = {
      case transfer: BankTransfer => transfer.status
    }

  }

}