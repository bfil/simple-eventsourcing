package io.bfil.eventsourcing

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class AggregateSpec extends WordSpec with Matchers with ScalaFutures {

  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  val customer = new CustomerAggregate("1")
                with InMemoryCustomersJournal
                with InMemoryCustomersCache

  "Aggregate" should {

    "create a customer correctly and throw an OptimisticLockException" in {
      val result1 = customer.create("Bruno")
      val result2 = customer.create("Bruno Mars")
      result1.futureValue shouldBe Customer("1", "Bruno")
      result2.failed.futureValue shouldBe an [OptimisticLockException]
      customer.state.futureValue shouldBe Customer("1", "Bruno")
    }

    "rename a customer correctly and retry on OptimisticLockException" in {
      val result1 = customer.rename("Bruno Mars")
      val result2 = customer.rename("Bruno")
      result1.futureValue shouldBe "Bruno Mars"
      result2.futureValue shouldBe "Bruno"
      customer.state.futureValue shouldBe Customer("1", "Bruno")
    }

  }

  sealed trait CustomerState extends AggregateState[CustomerEvent, CustomerState]
  case object Empty extends CustomerState {
    val eventHandler = EventHandler {
      case CustomerCreated(id, name) => Customer(id, name)
    }
  }
  case class Customer(id: String, name: String) extends CustomerState {
    val eventHandler = EventHandler {
      case CustomerRenamed(name) => copy(name = name)
    }
  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: String, name: String) extends CustomerEvent
  case class CustomerRenamed(name: String) extends CustomerEvent

  trait InMemoryCustomersJournal extends JournalProvider[CustomerEvent] {
    val journal: Journal[CustomerEvent] = new InMemoryJournal[CustomerEvent]
  }

  trait InMemoryCustomersCache extends CacheProvider[CustomerState] {
    val cache: Cache[CustomerState] = new InMemoryCache[CustomerState]
  }

  class CustomerAggregate(id: String)
    extends Aggregate[CustomerEvent, CustomerState] {
    self: JournalProvider[CustomerEvent] with CacheProvider[CustomerState] =>

    val aggregateId = s"customer-$id"
    val initialState = Empty

    private def recoverCustomer(): Future[Customer] =
      recover map {
        case customer: Customer => customer
        case _ => throw new Exception(s"Customer with id '$id' not found")
      }

    def create(name: String): Future[Customer] =
      for {
        state <- recover
        customer <- state match {
          case Empty => persist(state, CustomerCreated(id, name)).mapStateTo[Customer]
          case _     => Future.failed(new Exception(s"Customer with id '$id' already exists"))
        }
      } yield customer

    def rename(name: String): Future[String] = retry(2) {
      for {
        customer <- recoverCustomer
        renamedCustomer <- persist(customer, CustomerRenamed(name)).mapStateTo[Customer]
      } yield renamedCustomer.name
    }
  }
}
