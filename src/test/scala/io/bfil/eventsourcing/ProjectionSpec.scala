package io.bfil.eventsourcing

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var customerCount = 0
  val customerCountProjection = new CustomerCountProjection()
                               with FakeCustomerEventStream

  "Projection" should {

    "generate a customer count" in {
      customerCountProjection.run()
      eventually {
        customerCount shouldBe 10
      }
    }

  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: String, name: String) extends CustomerEvent
  case class CustomerRenamed(name: String) extends CustomerEvent

  case class Customer(id: String, name: String)

  trait FakeCustomerEventStream extends EventStreamProvider[CustomerEvent] {
    val eventStream: EventStream[CustomerEvent] = new EventStream[CustomerEvent] {
      def subscribe(f: CustomerEvent => Future[Unit], offset: Long = 0): Unit = 
        1 to 10 map { id =>
          f(CustomerCreated(s"customer-${id}", "Bruno"))
        }
    }
  }

  class CustomerCountProjection() extends Projection[CustomerEvent] {
    self: EventStreamProvider[CustomerEvent] =>

    def processEvent(event: CustomerEvent): Future[Unit] = event match {
      case CustomerCreated(id, name) => Future.successful(customerCount += 1)
      case                         _ => Future.successful(())
    }
  }
}
