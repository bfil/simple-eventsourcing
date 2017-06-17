package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class AggregateSpec extends WordSpec with Matchers with ScalaFutures with SingleThreadedExecutionContext {

  val journal: Journal[(BankAccountEvent, Long)] = new InMemoryJournalWithOptimisticLocking[BankAccountEvent]

  val bankAccount = new BankAccountAggregate(1, journal)

  "Aggregate" should {

    "open a bank account and throw an OptimisticLockException" in {
      val result1 = bankAccount.open("Bruno", 1000)
      val result2 = bankAccount.open("Bruno Mars", 1000)
      result1.futureValue shouldBe BankAccount(1, "Bruno", 1000)
      result2.failed.futureValue shouldBe an [OptimisticLockException]
      bankAccount.recover.futureValue shouldBe (Some(BankAccount(1, "Bruno", 1000)), 1)
      journal.read("bank-account-1").futureValue.length shouldBe 1
    }

    "withdraw money from a bank account and retry on OptimisticLockException" in {
      val result1 = bankAccount.withdraw(100)
      val result2 = bankAccount.withdraw(100)
      result1.futureValue shouldBe 900
      result2.futureValue shouldBe 800
      bankAccount.recover.futureValue shouldBe (Some(BankAccount(1, "Bruno", 800)), 3)
      journal.read("bank-account-1").futureValue.length shouldBe 3
    }

    "fail to withdraw money from a bank account when the balance is too low" in {
      an[Exception] should be thrownBy bankAccount.withdraw(1000).futureValue
      bankAccount.recover.futureValue shouldBe (Some(BankAccount(1, "Bruno", 800)), 3)
      journal.read("bank-account-1").futureValue.length shouldBe 3
    }

  }

  sealed trait BankAccountEvent
  case class BankAccountOpened(id: Int, name: String, balance: Int) extends BankAccountEvent
  case class MoneyWithdrawn(id: Int, amount: Int) extends BankAccountEvent

  case class BankAccount(id: Int, name: String, balance: Int)

  class BankAccountAggregate(id: Int, journal: Journal[(BankAccountEvent, Long)])
    extends Aggregate[BankAccountEvent, Option[BankAccount]](journal) {

    val aggregateId = s"bank-account-$id"
    val initialState = None

    def onEvent(state: Option[BankAccount], event: BankAccountEvent): Option[BankAccount] = event match {
      case BankAccountOpened(id, name, balance) => Some(BankAccount(id, name, balance))
      case MoneyWithdrawn(_, amount)            => state.map(a => a.copy(balance = a.balance - amount))
    }

    def open(name: String, balance: Int): Future[BankAccount] =
      for {
        (state, lastSequenceNr) <- recover
        newState <- state match {
          case Some(_) => Future.failed(new Exception(s"Bank account with id '$id' already exists"))
          case None    => persist(state, lastSequenceNr, BankAccountOpened(id, name, balance))
        }
      } yield newState.get

    def withdraw(amount: Int): Future[Int] = retry(2) {
      for {
        (state, lastSequenceNr) <- recover
        newState <- state match {
          case Some(bankAccount) =>
            if (bankAccount.balance >= amount) persist(state, lastSequenceNr, MoneyWithdrawn(id, amount))
            else Future.failed(new Exception(s"Not enough funds in account with id '$id'"))
          case None => Future.failed(new Exception(s"Bank account with id '$id' not found"))
        }
      } yield newState.get.balance
    }
  }
}
