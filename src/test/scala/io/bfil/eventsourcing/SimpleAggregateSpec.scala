package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class SimpleAggregateSpec extends WordSpec with Matchers with ScalaFutures with SingleThreadedExecutionContext {

  val journal: Journal[CustomerEvent] = new InMemoryJournal[CustomerEvent]

  val customer = new SimpleCustomerAggregate("1", journal)

  "SimpleAggregate" should {

    "create a customer correctly" in {
      customer.create("Bruno").futureValue shouldBe Customer("1", "Bruno")
      customer.recover.futureValue shouldBe Some(Customer("1", "Bruno"))
      journal.read("customer-1").futureValue.length shouldBe 1
    }

    "rename a customer correctly" in {
      customer.rename("Bruno Mars").futureValue shouldBe "Bruno Mars"
      customer.recover.futureValue shouldBe Some(Customer("1", "Bruno Mars"))
      journal.read("customer-1").futureValue.length shouldBe 2
    }

  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: String, name: String) extends CustomerEvent
  case class CustomerRenamed(name: String) extends CustomerEvent

  case class Customer(id: String, name: String)

  class SimpleCustomerAggregate(id: String, journal: Journal[CustomerEvent])
    extends SimpleAggregate[CustomerEvent, Option[Customer]](journal) {

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
