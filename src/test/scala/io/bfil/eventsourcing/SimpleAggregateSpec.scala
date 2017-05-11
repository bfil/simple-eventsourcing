package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class SimpleAggregateSpec extends WordSpec with Matchers with ScalaFutures {

  val customer = new SimpleCustomerAggregate("1")
                with InMemoryCustomersJournal

  "SimpleAggregate" should {

    "create a customer correctly" in {
      customer.create("Bruno").futureValue shouldBe Customer("1", "Bruno")
      customer.recover.futureValue shouldBe Some(Customer("1", "Bruno"))
    }

    "rename a customer correctly" in {
      customer.rename("Bruno Mars").futureValue shouldBe "Bruno Mars"
      customer.recover.futureValue shouldBe Some(Customer("1", "Bruno Mars"))
    }

  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: String, name: String) extends CustomerEvent
  case class CustomerRenamed(name: String) extends CustomerEvent

  case class Customer(id: String, name: String)

  trait InMemoryCustomersJournal extends JournalProvider[CustomerEvent] {
    val journal: Journal[CustomerEvent] = new InMemoryJournal[CustomerEvent]
  }

  class SimpleCustomerAggregate(id: String) extends SimpleAggregate[CustomerEvent, Option[Customer]] {
    self: JournalProvider[CustomerEvent] =>

    val aggregateId = s"customer-$id"
    val initialState = None

    def onEvent(state: Option[Customer], event: CustomerEvent): Option[Customer] = event match {
      case CustomerCreated(id, name) => Some(Customer(id, name))
      case CustomerRenamed(name)     => state.map(_.copy(name = name))
    }

    def create(name: String): Future[Customer] =
      for {
        state <- recover
        newState <- persist(state, CustomerCreated(id, name))
      } yield newState.get

    def rename(name: String): Future[String] =
      for {
        state <- recover
        newState <- persist(state, CustomerRenamed(name))
      } yield newState.get.name
  }
}
