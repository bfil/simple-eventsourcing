package io.bfil.eventsourcing

import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}

import org.scalatest.{Matchers, WordSpec}
import org.scalatest.concurrent.ScalaFutures

class AggregateSpec extends WordSpec with Matchers with ScalaFutures {

  implicit val executionContext = ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())

  val profile = new ProfileAggregate("1")
               with InMemoryProfilesJournal
               with InMemoryProfilesCache

  "Aggregate" should {

    "create a profile correctly and throw an OptimisticLockException" in {
      val result1 = profile.create("Bruno", 32)
      val result2 = profile.create("Bruno Mars", 32)
      result1.futureValue shouldBe Profile("1", "Bruno", 32)
      result2.failed.futureValue shouldBe an [OptimisticLockException]
      profile.state.futureValue shouldBe Profile("1", "Bruno", 32)
    }

    "rename a profile correctly and retry on OptimisticLockException" in {
      val result1 = profile.rename("Bruno Mars")
      val result2 = profile.rename("Bruno")
      result1.futureValue shouldBe "Bruno Mars"
      result2.futureValue shouldBe "Bruno"
      profile.state.futureValue shouldBe Profile("1", "Bruno", 32)
    }

  }

  sealed trait ProfileState extends AggregateState[ProfileEvent, ProfileState]
  case object Empty extends ProfileState {
    val eventHandler = EventHandler {
      case ProfileCreated(id, name, age) => Profile(id, name, age)
    }
  }
  case class Profile(id: String, name: String, age: Int) extends ProfileState {
    val eventHandler = EventHandler {
      case ProfileRenamed(name) => copy(name = name)
    }
  }

  sealed trait ProfileEvent
  case class ProfileCreated(id: String, name: String, age: Int) extends ProfileEvent
  case class ProfileRenamed(name: String) extends ProfileEvent

  trait InMemoryProfilesJournal extends JournalProvider[ProfileEvent] {
    val journal: Journal[ProfileEvent] = new InMemoryJournal[ProfileEvent]
  }

  trait InMemoryProfilesCache extends CacheProvider[ProfileState] {
    val cache: Cache[ProfileState] = new InMemoryCache[ProfileState]
  }

  class ProfileAggregate(id: String)
    extends Aggregate[ProfileEvent, ProfileState] {
    self: JournalProvider[ProfileEvent] with CacheProvider[ProfileState] =>

    val aggregateId = s"profile-$id"
    val initialState = Empty

    private def recoverProfile(): Future[Profile] =
      recover map {
        case profile: Profile => profile
        case _ => throw new Exception(s"Profile with id '$id' not found")
      }

    def create(name: String, age: Int): Future[Profile] =
      for {
        state <- recover
        profile <- state match {
          case Empty => persist(state, ProfileCreated(id, name, age)).mapStateTo[Profile]
          case _ => Future.failed(new Exception(s"Profile with id '$id' already exists"))
        }
      } yield profile

    def rename(name: String): Future[String] = retry(2) {
      for {
        profile <- recoverProfile
        renamedProfile <- persist(profile, ProfileRenamed(name)).mapStateTo[Profile]
      } yield renamedProfile.name
    }
  }
}
