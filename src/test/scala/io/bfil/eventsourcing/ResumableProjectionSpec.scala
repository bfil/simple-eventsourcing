package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ResumableProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var customerCount = 0

  val offsetStore = new InMemoryOffsetStore
  val eventStream = new InMemoryEventStream[EventEnvelope[CustomerEvent]]()

  "ResumableProjection" should {

    "generate a customer count and store the last offset in the offset store" in {
      val customerCountProjection = new CustomerCountResumableProjection(eventStream, offsetStore)
      customerCountProjection.run()
      1 to 10 map { id =>
        eventStream.publish(EventEnvelope(id, "customer-1", CustomerCreated(id.toString, "Bruno")))
      }
      eventually {
        customerCount shouldBe 10
        offsetStore.load("customer-count").futureValue shouldBe 10
      }
    }

    "resume from the last saved offset" in {
      val customerCountProjection = new CustomerCountResumableProjection(eventStream, offsetStore)
      customerCountProjection.run()
      11 to 20 map { id =>
        eventStream.publish(EventEnvelope(id, "customer-1", CustomerCreated(id.toString, "Bruno")))
      }
      eventually {
        customerCount shouldBe 20
        offsetStore.load("customer-count").futureValue shouldBe 20
      }
    }

  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: String, name: String) extends CustomerEvent
  case class CustomerRenamed(name: String) extends CustomerEvent

  case class Customer(id: String, name: String)

  class CustomerCountResumableProjection(eventStream: EventStream[EventEnvelope[CustomerEvent]], offsetStore: OffsetStore)
    extends ResumableProjection[CustomerEvent](eventStream, offsetStore) {
    val projectionId = "customer-count"

    def processEvent(event: CustomerEvent): Future[Unit] = event match {
      case CustomerCreated(id, name) => Future.successful(customerCount += 1)
      case                         _ => Future.successful(())
    }
  }
}
