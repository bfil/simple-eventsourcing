package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class AggregateSpec extends WordSpec with Matchers with ScalaFutures with SingleThreadedExecutionContext {

  val journal: Journal[BankAccountEvent] = new InMemoryJournal[BankAccountEvent]
  val cache: Cache[BankAccountState] = new InMemoryCache[BankAccountState]

  val bankAccount = new BankAccountAggregate(1, journal, cache)

  "Aggregate" should {

    "open a bank account and throw an OptimisticLockException" in {
      val result1 = bankAccount.open("Bruno", 1000)
      val result2 = bankAccount.open("Bruno Mars", 1000)
      result1.futureValue shouldBe BankAccount(1, "Bruno", 1000)
      result2.failed.futureValue shouldBe an [OptimisticLockException]
      bankAccount.state.futureValue shouldBe BankAccount(1, "Bruno", 1000)
      journal.read("bank-account-1").futureValue.length shouldBe 1
    }

    "withdraw money from a bank account and retry on OptimisticLockException" in {
      val result1 = bankAccount.withdraw(100)
      val result2 = bankAccount.withdraw(100)
      result1.futureValue shouldBe 900
      result2.futureValue shouldBe 800
      bankAccount.state.futureValue shouldBe BankAccount(1, "Bruno", 800)
      journal.read("bank-account-1").futureValue.length shouldBe 3
    }

    "fail to withdraw money from a bank account when the balance is too low" in {
      an[Exception] should be thrownBy bankAccount.withdraw(1000).futureValue
      bankAccount.state.futureValue shouldBe BankAccount(1, "Bruno", 800)
      journal.read("bank-account-1").futureValue.length shouldBe 3
    }

  }

  sealed trait BankAccountState extends AggregateState[BankAccountEvent, BankAccountState]
  case object Empty extends BankAccountState {
    val eventHandler = EventHandler {
      case BankAccountOpened(id, name, balance) => BankAccount(id, name, balance)
    }
  }
  case class BankAccount(id: Int, name: String, balance: Int) extends BankAccountState {
    val eventHandler = EventHandler {
      case MoneyWithdrawn(id, amount) => copy(balance = balance - amount)
    }
  }

  sealed trait BankAccountEvent
  case class BankAccountOpened(id: Int, name: String, balance: Int) extends BankAccountEvent
  case class MoneyWithdrawn(id: Int, amount: Int) extends BankAccountEvent

  class BankAccountAggregate(id: Int, journal: Journal[BankAccountEvent], cache: Cache[BankAccountState])
    extends Aggregate[BankAccountEvent, BankAccountState](journal, cache) {

    val aggregateId = s"bank-account-$id"
    val initialState = Empty

    private def recoverBankAccount(): Future[BankAccount] =
      recover map {
        case bankAccount: BankAccount => bankAccount
        case _                        => throw new Exception(s"Bank account with id '$id' not found")
      }

    def open(name: String, balance: Int): Future[BankAccount] =
      for {
        state       <- recover
        bankAccount <- state match {
          case Empty => persist(state, BankAccountOpened(id, name, balance)).mapStateTo[BankAccount]
          case _     => Future.failed(new Exception(s"Bank account with id '$id' already exists"))
        }
      } yield bankAccount

    def withdraw(amount: Int): Future[Int] = retry(2) {
      for {
        bankAccount        <- recoverBankAccount
        updatedBankAccount <-
          if(bankAccount.balance >= amount) {
            persist(bankAccount, MoneyWithdrawn(id, amount)).mapStateTo[BankAccount]
          } else Future.failed(new Exception(s"Not enough funds in account with id '$id'"))
      } yield updatedBankAccount.balance
    }
  }
}
