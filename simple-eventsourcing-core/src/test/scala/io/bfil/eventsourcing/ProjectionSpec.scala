package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  val eventStream = new InMemoryEventStream[BankAccountEvent]()

  val projection = new BankAccountCountProjection(eventStream)

  "Projection" should {

    "count the number of opened bank accounts" in {
      projection.run()
      1 to 10 map { id =>
        eventStream.publish(EventEnvelope(id, "bank-account-1", BankAccountOpened(id, "Bruno", 1000)))
      }
      eventually {
        projection.bankAccountCount shouldBe 10
      }
    }

  }

  sealed trait BankAccountEvent
  case class BankAccountOpened(id: Int, name: String, balance: Int) extends BankAccountEvent
  case class MoneyWithdrawn(id: Int, amount: Int) extends BankAccountEvent

  class BankAccountCountProjection(eventStream: EventStream[BankAccountEvent]) extends Projection[BankAccountEvent](eventStream) {
    var bankAccountCount = 0

    def processEvent(event: BankAccountEvent): Future[Unit] = event match {
      case BankAccountOpened(id, name, balance) => Future.successful(bankAccountCount += 1)
      case _                                    => Future.successful(())
    }
  }
}
