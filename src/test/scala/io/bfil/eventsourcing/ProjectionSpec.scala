package io.bfil.eventsourcing

import java.util.concurrent.LinkedBlockingQueue

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var customerCount = 0
  val queue = new LinkedBlockingQueue[CustomerEvent]()
  val customerCountProjection = new CustomerCountProjection()
                               with EventStreamProvider[CustomerEvent] {
                                 val eventStream = new BlockingQueueEventStream(queue)
                               }

  "Projection" should {

    "generate a customer count" in {
      customerCountProjection.run()
      1 to 10 map { id =>
        queue.put(CustomerCreated(s"customer-$id", "Bruno"))
      }
      eventually {
        customerCount shouldBe 10
      }
    }

  }

  sealed trait CustomerEvent
  case class CustomerCreated(id: String, name: String) extends CustomerEvent
  case class CustomerRenamed(name: String) extends CustomerEvent

  case class Customer(id: String, name: String)

  class CustomerCountProjection() extends Projection[CustomerEvent] {
    self: EventStreamProvider[CustomerEvent] =>

    def processEvent(event: CustomerEvent): Future[Unit] = event match {
      case CustomerCreated(id, name) => Future.successful(customerCount += 1)
      case                         _ => Future.successful(())
    }
  }
}
