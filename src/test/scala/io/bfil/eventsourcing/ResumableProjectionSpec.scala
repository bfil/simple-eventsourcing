package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ResumableProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var customerCount = 0

  val offsetStore = new InMemoryOffsetStore
  val eventStream = new InMemoryEventStream[CustomerEvent]()

  "ResumableProjection" should {

    "generate a customer count and store the last offset in the offset store" in {
      val customerCountProjection = new CustomerCountResumableProjection(eventStream, offsetStore)
      customerCountProjection.run()
      1 to 10 map { id =>
        eventStream.publish(EventEnvelope(id, "customer-1", CustomerCreated(id, "Bruno", 32)))
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
        eventStream.publish(EventEnvelope(id, "customer-1", CustomerCreated(id, "Bruno", 32)))
      }
      eventually {
        customerCount shouldBe 20
        offsetStore.load("customer-count").futureValue shouldBe 20
      }
    }

  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: Int, name: String, age: Int) extends CustomerEvent
  case class CustomerRenamed(id: Int, name: String) extends CustomerEvent

  class CustomerCountResumableProjection(eventStream: EventStream[CustomerEvent], offsetStore: OffsetStore)
    extends ResumableProjection[CustomerEvent](eventStream, offsetStore) {
    val projectionId = "customer-count"

    def processEvent(event: CustomerEvent): Future[Unit] = event match {
      case CustomerCreated(id, name, age) => Future.successful(customerCount += 1)
      case                              _ => Future.successful(())
    }
  }
}
