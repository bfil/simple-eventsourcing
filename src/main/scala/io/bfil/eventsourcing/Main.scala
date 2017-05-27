package io.bfil.eventsourcing

import java.util.logging.{Level, Logger}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import io.circe.generic.auto._
import io.bfil.eventsourcing.circe.JsonEncoding
import io.bfil.eventsourcing.mongodb._
import io.bfil.eventsourcing.inmemory.InMemoryCache
import io.bfil.eventsourcing.serialization._
import org.mongodb.scala._
import org.mongodb.scala.model._

object Main extends App {
  Logger.getLogger("org.mongodb.driver").setLevel(Level.OFF)

  val mongoClient = MongoClient("mongodb://localhost")
  val database = mongoClient.getDatabase("simple-eventsourcing")
  val journalCollection = database.getCollection("journal")
  val offsetsCollection = database.getCollection("offsets")
  val customersCollection = database.getCollection("customers")

  implicit val customerEventSerializer = new CustomerEventSerializer

  val journalWriter = new MongoJournalWriter(journalCollection)
  val journal = new MongoJournal[CustomerEvent](journalCollection, journalWriter)
  val cache = new InMemoryCache[CustomerState]

  1 to 100 foreach { id =>
    val customer = new CustomerAggregate(id, journal, cache)
    (for {
      cust <- customer.create("Bruno", 32)
      name <- customer.rename("Bruno Mars")
      name <- customer.rename("Bruno")
    } yield ()).failed foreach println
    customer.state
  }

  val offsetStore = new MongoOffsetStore(offsetsCollection)
  val journalEventStream = new MongoPollingEventStream[CustomerEvent](journalCollection)
  val customersProjection = new CustomersProjection(customersCollection, journalEventStream, offsetStore)

  val start = System.currentTimeMillis
  customersProjection.run()

  import scala.concurrent.Await
  import scala.concurrent.duration._
  while (Await.result(offsetStore.load("customers-projection"), 3 second) != 300) {
    Thread.sleep(100)
  }

  println(s"Projection run in ${System.currentTimeMillis - start}ms")
  journalWriter.shutdown()
  journalEventStream.shutdown()
}

sealed trait CustomerState extends AggregateState[CustomerEvent, CustomerState]
case object Empty extends CustomerState {
  val eventHandler = EventHandler {
    case CustomerCreated(id, name, age) => Customer(id, name, age)
  }
}
case class Customer(id: Int, name: String, age: Int) extends CustomerState {
  val eventHandler = EventHandler {
    case CustomerRenamed(id, name) => copy(name = name)
  }
}

sealed trait CustomerEvent
case class CustomerCreated(id: Int, name: String, age: Int) extends CustomerEvent
case class CustomerRenamed(id: Int, name: String) extends CustomerEvent

class CustomerEventSerializer extends EventSerializer[CustomerEvent] {
  import JsonEncoding._
  def serialize(event: CustomerEvent) = event match {
    case event: CustomerCreated => SerializedEvent("CustomerCreated.V1", encode(event))
    case event: CustomerRenamed => SerializedEvent("CustomerRenamed.V1", encode(event))
  }
  def deserialize(manifest: String, data: String) = manifest match {
    case "CustomerCreated.V1" => decode[CustomerCreated](data)
    case "CustomerRenamed.V1" => decode[CustomerRenamed](data)
  }
}

class CustomerAggregate(id: Int, journal: Journal[CustomerEvent], cache: Cache[CustomerState])
  extends Aggregate[CustomerEvent, CustomerState](journal, cache) {

  val aggregateId = s"customer-$id"
  val initialState = Empty

  private def recoverCustomer(): Future[Customer] =
    recover map {
      case customer: Customer => customer
      case _ => throw new Exception(s"Customer with id '$id' not found")
    }

  def create(name: String, age: Int): Future[Customer] =
    for {
      state <- recover
      customer <- state match {
        case Empty => persist(state, CustomerCreated(id, name, age)).mapStateTo[Customer]
        case _     => Future.failed(new Exception(s"Customer with id '$id' already exists"))
      }
    } yield customer

  def rename(name: String): Future[String] = retry(1) {
    for {
      customer <- recoverCustomer
      renamedCustomer <- persist(customer, CustomerRenamed(id, name)).mapStateTo[Customer]
    } yield renamedCustomer.name
  }
}

class CustomersProjection(
  collection: MongoCollection[Document],
  eventStream: EventStream[EventEnvelope[CustomerEvent]],
  offsetStore: OffsetStore
  ) extends ResumableProjection[CustomerEvent](eventStream, offsetStore) {
  val projectionId = "customers-projection"

  collection.createIndex(Indexes.ascending("id")).toFuture()

  def processEvent(event: CustomerEvent): Future[Unit] = event match {
    case CustomerCreated(id, name, age) =>
      val customer = Document("id" -> id, "name" -> name, "age" -> age)
      val updateOptions = new UpdateOptions().upsert(true)
      collection.updateOne(Filters.equal("id", id), Document("$set" -> customer), updateOptions)
                .toFuture()
                .map(completed => ())
    case CustomerRenamed(id, name) =>
      val customer = Document("id" -> id, "name" -> name)
      collection.updateOne(Filters.equal("id", id), Document("$set" -> customer))
                .toFuture()
                .map(completed => ())
  }
}
