package io.bfil.eventsourcing

import inmemory._

import scala.concurrent.Future

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

class ResumableProjectionSpec extends WordSpec with Matchers with ScalaFutures with Eventually with SingleThreadedExecutionContext {

  var bankAccountCount = 0

  val offsetStore = new InMemoryOffsetStore

  "ResumableProjection" should {

    "count the number of opened bank accounts and store the last offset in the offset store" in {
      val eventStream = new InMemoryEventStream[BankAccountEvent]()
      val projection = new BankAccountCountResumableProjection(eventStream, offsetStore)
      projection.run()
      1 to 10 map { id =>
        eventStream.publish(EventEnvelope(id, "bank-account-1", BankAccountOpened(id, "Bruno", 1000)))
      }
      eventually {
        bankAccountCount shouldBe 10
        offsetStore.load("bank-account-count").futureValue shouldBe 10
      }
    }

    "resume from the last saved offset" in {
      val eventStream = new InMemoryEventStream[BankAccountEvent]()
      val projection = new BankAccountCountResumableProjection(eventStream, offsetStore)
      projection.run()
      bankAccountCount shouldBe 10
      1 to 20 map { id =>
        eventStream.publish(EventEnvelope(id, "bank-account-1", BankAccountOpened(id, "Bruno", 1000)))
      }
      eventually {
        bankAccountCount shouldBe 20
        offsetStore.load("bank-account-count").futureValue shouldBe 20
      }
    }

  }

  sealed trait BankAccountEvent
  case class BankAccountOpened(id: Int, name: String, balance: Int) extends BankAccountEvent
  case class MoneyWithdrawn(id: Int, amount: Int) extends BankAccountEvent

  class BankAccountCountResumableProjection(eventStream: EventStream[BankAccountEvent], offsetStore: OffsetStore)
    extends ResumableProjection[BankAccountEvent](eventStream, offsetStore) {
    val projectionId = "bank-account-count"

    def processEvent(event: BankAccountEvent): Future[Unit] = event match {
      case BankAccountOpened(id, name, balance) => Future.successful(bankAccountCount += 1)
      case                              _ => Future.successful(())
    }
  }
}
