package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class SnapshottingAggregateSpec extends WordSpec with Matchers with ScalaFutures with SingleThreadedExecutionContext {

  val journal: JournalWithOptimisticLocking[BankAccountEvent] = new InMemoryJournalWithOptimisticLocking[BankAccountEvent]
  val snapshotStore: SnapshotStore[Option[BankAccount]] = new InMemorySnapshotStore[Option[BankAccount]]

  val bankAccount = new BankAccountAggregate(1, journal, snapshotStore)

  "SnapshottingAggregate" should {

    "snapshot after 10 events" in {
      bankAccount.open("Bruno", 1000).futureValue shouldBe BankAccount(1, "Bruno", 1000)
      1 to 9 foreach { i =>
        bankAccount.withdraw(100).futureValue shouldBe (1000 - 100 * i)
      }
      journal.read("bank-account-1").futureValue.length shouldBe 10
      snapshotStore.load("bank-account-1").futureValue shouldBe Some(Snapshot(Some(BankAccount(1, "Bruno", 100)), 10))
    }

    "recover from the last snapshot" in {
      bankAccount.recover.futureValue shouldBe VersionedState(Some(BankAccount(1, "Bruno", 100)), 10)
    }

  }

  sealed trait BankAccountEvent
  case class BankAccountOpened(id: Int, name: String, balance: Int) extends BankAccountEvent
  case class MoneyWithdrawn(id: Int, amount: Int) extends BankAccountEvent

  case class BankAccount(id: Int, name: String, balance: Int)

  class BankAccountAggregate(
    id: Int,
    journal: JournalWithOptimisticLocking[BankAccountEvent],
    snapshotStore: SnapshotStore[Option[BankAccount]]
    ) extends SnapshottingAggregate[BankAccountEvent, Option[BankAccount]](journal, snapshotStore, 10) {

    val aggregateId = s"bank-account-$id"
    val initialState = None

    def onEvent(state: Option[BankAccount], event: BankAccountEvent): Option[BankAccount] = event match {
      case BankAccountOpened(id, name, balance) => Some(BankAccount(id, name, balance))
      case MoneyWithdrawn(_, amount)            => state.map(a => a.copy(balance = a.balance - amount))
    }

    def open(name: String, balance: Int): Future[BankAccount] =
      for {
        versionedState <- recover
        newState <- versionedState.state match {
          case Some(_) => Future.failed(new Exception(s"Bank account with id '$id' already exists"))
          case None    => persist(versionedState, BankAccountOpened(id, name, balance))
        }
      } yield newState.get

    def withdraw(amount: Int): Future[Int] = retry(2) {
      for {
        versionedState <- recover
        newState <- versionedState.state match {
          case Some(bankAccount) =>
            if (bankAccount.balance >= amount) persist(versionedState, MoneyWithdrawn(id, amount))
            else Future.failed(new Exception(s"Not enough funds in account with id '$id'"))
          case None => Future.failed(new Exception(s"Bank account with id '$id' not found"))
        }
      } yield newState.get.balance
    }
  }
}
