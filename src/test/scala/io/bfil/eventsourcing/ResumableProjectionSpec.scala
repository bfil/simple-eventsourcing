package io.bfil.eventsourcing

import java.util.concurrent.LinkedBlockingQueue

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ResumableProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var customerCount = 0
  val queue = new LinkedBlockingQueue[CustomerEvent]()
  val customerCountProjection = new CustomerCountResumableProjection()
                               with EventStreamProvider[CustomerEvent]
                               with InMemoryCustomerCountOffsetStore {
                                 val eventStream = new BlockingQueueEventStream(queue)
                               }

  "ResumableProjection" should {

    "generate a customer count and store the last offset in the offset store" in {
      customerCountProjection.run()
      1 to 10 map { id =>
        queue.put(CustomerCreated(s"customer-$id", "Bruno", id))
      }
      eventually {
        customerCount shouldBe 10
      }
      customerCountProjection.offsetStore.load("customer-count").futureValue shouldBe 10
    }

  }

  sealed trait CustomerEvent { val offset: Long }
  case class CustomerCreated(id: String, name: String, val offset: Long) extends CustomerEvent
  case class CustomerRenamed(name: String, val offset: Long) extends CustomerEvent

  case class Customer(id: String, name: String)

  trait InMemoryCustomerCountOffsetStore extends OffsetStoreProvider {
    val offsetStore = new InMemoryOffsetStore
  }

  class CustomerCountResumableProjection() extends ResumableProjection[CustomerEvent] {
    self: EventStreamProvider[CustomerEvent] with OffsetStoreProvider =>

    val projectionId = "customer-count"

    def processEvent(event: CustomerEvent): Future[Unit] = event match {
      case CustomerCreated(id, name, _) => Future.successful(customerCount += 1)
      case                            _ => Future.successful(())
    }

    def getEventOffset(event: CustomerEvent): Long = event.offset
  }
}
