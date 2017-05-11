package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class SimpleAggregateSpec extends WordSpec with Matchers with ScalaFutures {

  val profile = new SimpleProfileAggregate("1")
               with InMemoryProfilesJournal

  "SimpleAggregate" should {

    "create a profile correctly" in {
      profile.create("Bruno", 32).futureValue shouldBe Profile("1", "Bruno", 32)
      profile.recover.futureValue shouldBe Some(Profile("1", "Bruno", 32))
    }

    "rename a profile correctly" in {
      profile.rename("Bruno Mars").futureValue shouldBe "Bruno Mars"
      profile.recover.futureValue shouldBe Some(Profile("1", "Bruno Mars", 32))
    }

  }

  sealed trait ProfileEvent
  case class ProfileCreated(id: String, name: String, age: Int) extends ProfileEvent
  case class ProfileRenamed(name: String) extends ProfileEvent

  case class Profile(id: String, name: String, age: Int)

  trait InMemoryProfilesJournal extends JournalProvider[ProfileEvent] {
    val journal: Journal[ProfileEvent] = new InMemoryJournal[ProfileEvent]
  }

  class SimpleProfileAggregate(id: String) extends SimpleAggregate[ProfileEvent, Option[Profile]] {
    self: JournalProvider[ProfileEvent] =>

    val aggregateId = s"profile-$id"
    val initialState = None

    def onEvent(state: Option[Profile], event: ProfileEvent): Option[Profile] = event match {
      case ProfileCreated(id, name, age) => Some(Profile(id, name, age))
      case ProfileRenamed(name)          => state.map(_.copy(name = name))
    }

    def create(name: String, age: Int): Future[Profile] =
      for {
        state <- recover
        newState <- persist(state, ProfileCreated(id, name, age))
      } yield newState.get

    def rename(name: String): Future[String] =
      for {
        state <- recover
        newState <- persist(state, ProfileRenamed(name))
      } yield newState.get.name
  }
}
