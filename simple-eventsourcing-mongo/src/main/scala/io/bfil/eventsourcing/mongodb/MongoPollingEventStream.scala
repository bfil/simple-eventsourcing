package io.bfil.eventsourcing.mongodb

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import io.bfil.eventsourcing.{EventEnvelope, PollingEventStream}
import io.bfil.eventsourcing.serialization.EventSerializer
import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}
import org.mongodb.scala.model._

class MongoPollingEventStream[Event](
  collection: MongoCollection[Document]
  )(implicit serializer: EventSerializer[Event]) extends PollingEventStream[Event](500 millis) {

  def poll(offset: Long): Future[Seq[EventEnvelope[Event]]] =
    collection.find(Filters.gt("offset", offset))
              .sort(Sorts.ascending("offset"))
              .limit(100)
              .toFuture()
              .map(_.map(documentToEventEnvelope))

  private def documentToEventEnvelope(doc: Document) = {
    val offset = doc[BsonInt64]("offset").getValue()
    val aggregateId = doc[BsonString]("aggregateId").getValue()
    val manifest = doc[BsonString]("manifest").getValue()
    val data = doc[BsonDocument]("data")
    val timestamp = doc[BsonString]("timestamp").getValue()
    val event = serializer.deserialize(manifest, data.toJson())
    EventEnvelope(
      offset = offset,
      aggregateId = aggregateId,
      event = event,
      timestamp = ZonedDateTime.parse(timestamp, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
                               .toInstant()
    )
  }

}
