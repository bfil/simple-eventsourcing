package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class SimpleAggregateSpec extends WordSpec with Matchers with ScalaFutures with SingleThreadedExecutionContext {

  val journal: Journal[BankAccountEvent] = new InMemoryJournal[BankAccountEvent]

  val bankAccount = new SimpleBankAccountAggregate(1, journal)

  "SimpleAggregate" should {

    "open a bank account" in {
      bankAccount.open("Bruno", 1000).futureValue shouldBe BankAccount(1, "Bruno", 1000)
      bankAccount.recover.futureValue shouldBe Some(BankAccount(1, "Bruno", 1000))
      journal.read("bank-account-1").futureValue.length shouldBe 1
    }

    "withdraw money from a bank account" in {
      bankAccount.withdraw(100).futureValue shouldBe 900
      bankAccount.recover.futureValue shouldBe Some(BankAccount(1, "Bruno", 900))
      journal.read("bank-account-1").futureValue.length shouldBe 2
    }

    "fail to withdraw money from a bank account when the balance is too low" in {
      an[Exception] should be thrownBy bankAccount.withdraw(1000).futureValue
      bankAccount.recover.futureValue shouldBe Some(BankAccount(1, "Bruno", 900))
      journal.read("bank-account-1").futureValue.length shouldBe 2
    }

  }

  sealed trait BankAccountEvent
  case class BankAccountOpened(id: Int, name: String, balance: Int) extends BankAccountEvent
  case class MoneyWithdrawn(id: Int, amount: Int) extends BankAccountEvent

  case class BankAccount(id: Int, name: String, balance: Int)

  class SimpleBankAccountAggregate(id: Int, journal: Journal[BankAccountEvent])
    extends SimpleAggregate[BankAccountEvent, Option[BankAccount]](journal) {

    val aggregateId = s"bank-account-$id"
    val initialState = None

    def onEvent(state: Option[BankAccount], event: BankAccountEvent): Option[BankAccount] = event match {
      case BankAccountOpened(id, name, balance) => Some(BankAccount(id, name, balance))
      case MoneyWithdrawn(id, amount)           => state.map(a => a.copy(balance = a.balance - amount))
    }

    def open(name: String, balance: Int): Future[BankAccount] =
      for {
        state    <- recover
        newState <- state match {
          case Some(bankAccount) => Future.failed(new Exception(s"Bank account with id '$id' already exists"))
          case None              => persist(state, BankAccountOpened(id, name, balance))
        }
      } yield newState.get

    def withdraw(amount: Int): Future[Int] =
      for {
        state    <- recover
        newState <- state match {
          case Some(bankAccount) =>
            if(bankAccount.balance >= amount) persist(state, MoneyWithdrawn(id, amount))
            else Future.failed(new Exception(s"Not enough funds in account with id '$id'"))
          case None => Future.failed(new Exception(s"Bank account with id '$id' not found"))
        }
      } yield newState.get.balance
  }
}
