package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class AggregateSpec extends WordSpec with Matchers with ScalaFutures with SingleThreadedExecutionContext {

  val journal: Journal[CustomerEvent] = new InMemoryJournal[CustomerEvent]
  val cache: Cache[CustomerState] = new InMemoryCache[CustomerState]

  val customer = new CustomerAggregate(1, journal, cache)

  "Aggregate" should {

    "create a customer correctly and throw an OptimisticLockException" in {
      val result1 = customer.create("Bruno", 32)
      val result2 = customer.create("Bruno Mars", 32)
      result1.futureValue shouldBe Customer(1, "Bruno", 32)
      result2.failed.futureValue shouldBe an [OptimisticLockException]
      customer.state.futureValue shouldBe Customer(1, "Bruno", 32)
      journal.read("customer-1").futureValue.length shouldBe 1
    }

    "rename a customer correctly and retry on OptimisticLockException" in {
      val result1 = customer.rename("Bruno Mars")
      val result2 = customer.rename("Bruno")
      result1.futureValue shouldBe "Bruno Mars"
      result2.futureValue shouldBe "Bruno"
      customer.state.futureValue shouldBe Customer(1, "Bruno", 32)
      journal.read("customer-1").futureValue.length shouldBe 3
    }

  }

  sealed trait CustomerState extends AggregateState[CustomerEvent, CustomerState]
  case object Empty extends CustomerState {
    val eventHandler = EventHandler {
      case CustomerCreated(id, name, age) => Customer(id, name, age)
    }
  }
  case class Customer(id: Int, name: String, age: Int) extends CustomerState {
    val eventHandler = EventHandler {
      case CustomerRenamed(id, name) => copy(name = name)
    }
  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: Int, name: String, age: Int) extends CustomerEvent
  case class CustomerRenamed(id: Int, name: String) extends CustomerEvent

  class CustomerAggregate(id: Int, journal: Journal[CustomerEvent], cache: Cache[CustomerState])
    extends Aggregate[CustomerEvent, CustomerState](journal, cache) {

    val aggregateId = s"customer-$id"
    val initialState = Empty

    private def recoverCustomer(): Future[Customer] =
      recover map {
        case customer: Customer => customer
        case _ => throw new Exception(s"Customer with id '$id' not found")
      }

    def create(name: String, age: Int): Future[Customer] =
      for {
        state <- recover
        customer <- state match {
          case Empty => persist(state, CustomerCreated(id, name, age)).mapStateTo[Customer]
          case _     => Future.failed(new Exception(s"Customer with id '$id' already exists"))
        }
      } yield customer

    def rename(name: String): Future[String] = retry(2) {
      for {
        customer <- recoverCustomer
        renamedCustomer <- persist(customer, CustomerRenamed(id, name)).mapStateTo[Customer]
      } yield renamedCustomer.name
    }
  }
}
