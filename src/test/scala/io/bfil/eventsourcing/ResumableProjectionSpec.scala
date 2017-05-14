package io.bfil.eventsourcing

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ResumableProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var customerCount = 0
  val customerCountProjection = new CustomerCountResumableProjection()
                                        with FakeCustomerEventStream
                                        with InMemoryOffsetStore

  "ResumableProjection" should {

    "generate a customer count and store the last offset in the offset store" in {
      customerCountProjection.run()
      eventually {
        customerCount shouldBe 10
      }
      customerCountProjection.offsetStore.read("customer-count").futureValue shouldBe 10
    }

  }

  sealed trait CustomerEvent { val offset: Long }
  case class CustomerCreated(id: String, name: String, val offset: Long) extends CustomerEvent
  case class CustomerRenamed(name: String, val offset: Long) extends CustomerEvent

  case class Customer(id: String, name: String)

  trait FakeCustomerEventStream extends EventStreamProvider[CustomerEvent] {
    val eventStream: EventStream[CustomerEvent] = new EventStream[CustomerEvent] {
      def subscribe(f: CustomerEvent => Future[Unit], offset: Long = 0): Unit =
        1 to 10 map { id =>
          f(CustomerCreated(s"customer-${id}", "Bruno", id))
        }
    }
  }

  trait InMemoryOffsetStore extends OffsetStoreProvider {
    val offsetStore = new OffsetStore {
      var offset: Long = 0
      def read(offsetId: String): Future[Long] = Future.successful(offset)
      def write(offsetId: String, value: Long): Future[Unit] = Future.successful(offset = value)
    }
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
