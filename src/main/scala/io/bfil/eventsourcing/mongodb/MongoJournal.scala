package io.bfil.eventsourcing.mongodb

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.concurrent.{ExecutionContext, Future}

import io.bfil.eventsourcing.Journal
import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonDocument, BsonString}
import org.mongodb.scala.model._

class MongoJournal[Event](
  collection: MongoCollection[Document],
  offset: MongoJournalOffset
  )(implicit
    executionContext: ExecutionContext,
    serializer: MongoJournalEventSerializer[Event]
  ) extends Journal[Event] {

  collection.createIndex(Indexes.ascending("offset")).toFuture()
  collection.createIndex(Indexes.ascending("aggregateId")).toFuture()

  def read(aggregateId: String): Future[Seq[Event]] =
    collection.find(Document("aggregateId" -> aggregateId))
              .sort(Sorts.ascending("offset"))
              .toFuture()
              .map { docs =>
                docs.map { doc =>
                  val manifest = doc[BsonString]("manifest").getValue()
                  val data = doc[BsonDocument]("data")
                  serializer.deserialize(manifest, data.toJson())
                }
              }

  def write(aggregateId: String, events: Seq[Event]): Future[Unit] = {
    val documents = events map { event =>
      val (manifest, data) = serializer.serialize(event)
      Document(
        "offset" -> offset.next(),
        "aggregateId" -> aggregateId,
        "manifest" -> manifest,
        "data" -> Document(data),
        "timestamp" -> Instant.now.atZone(ZoneOffset.UTC)
                                  .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
      )
    }
    collection.insertMany(documents)
              .toFuture()
              .map(completed => ())
  }
}
