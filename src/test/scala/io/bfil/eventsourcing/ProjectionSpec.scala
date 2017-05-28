package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  val eventStream = new InMemoryEventStream[CustomerEvent]()

  val customerCountProjection = new CustomerCountProjection(eventStream)

  "Projection" should {

    "generate a customer count" in {
      customerCountProjection.run()
      1 to 10 map { id =>
        eventStream.publish(EventEnvelope(id, "customer-1", CustomerCreated(id, "Bruno", 32)))
      }
      eventually {
        customerCountProjection.customerCount shouldBe 10
      }
    }

  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: Int, name: String, age: Int) extends CustomerEvent
  case class CustomerRenamed(id: Int, name: String) extends CustomerEvent

  class CustomerCountProjection(eventStream: EventStream[CustomerEvent]) extends Projection[CustomerEvent](eventStream) {
    var customerCount = 0

    def processEvent(event: CustomerEvent): Future[Unit] = event match {
      case CustomerCreated(id, name, age) => Future.successful(customerCount += 1)
      case                              _ => Future.successful(())
    }
  }
}
