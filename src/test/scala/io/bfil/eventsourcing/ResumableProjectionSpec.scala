package io.bfil.eventsourcing

import java.util.concurrent.LinkedBlockingQueue

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ResumableProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var customerCount = 0

  val offsetStore = new InMemoryOffsetStore

  "ResumableProjection" should {

    "generate a customer count and store the last offset in the offset store" in {
      val queue = new LinkedBlockingQueue[CustomerEvent]()
      val eventStream = new BlockingQueueEventStream(queue)
      val customerCountProjection = new CustomerCountResumableProjection(eventStream, offsetStore)
      customerCountProjection.run()
      1 to 10 map { id =>
        queue.put(CustomerCreated(s"customer-$id", "Bruno", id))
      }
      eventually {
        customerCount shouldBe 10
      }
      offsetStore.load("customer-count").futureValue shouldBe 10
    }

    "resume from the last saved offset and ignore older events" in {
      val queue = new LinkedBlockingQueue[CustomerEvent]()
      val eventStream = new BlockingQueueEventStream(queue)
      val customerCountProjection = new CustomerCountResumableProjection(eventStream, offsetStore)
      customerCountProjection.run()
      6 to 20 map { id =>
        queue.put(CustomerCreated(s"customer-$id", "Bruno", id))
      }
      eventually {
        customerCount shouldBe 20
      }
      offsetStore.load("customer-count").futureValue shouldBe 20
    }

  }

  sealed trait CustomerEvent { val offset: Long }
  case class CustomerCreated(id: String, name: String, val offset: Long) extends CustomerEvent
  case class CustomerRenamed(name: String, val offset: Long) extends CustomerEvent

  case class Customer(id: String, name: String)

  class CustomerCountResumableProjection(eventStream: EventStream[CustomerEvent], offsetStore: OffsetStore)
    extends ResumableProjection[CustomerEvent](eventStream, offsetStore) {
    val projectionId = "customer-count"

    def processEvent(event: CustomerEvent): Future[Unit] = event match {
      case CustomerCreated(id, name, _) => Future.successful(customerCount += 1)
      case                            _ => Future.successful(())
    }

    def getEventOffset(event: CustomerEvent): Long = event.offset
  }
}
