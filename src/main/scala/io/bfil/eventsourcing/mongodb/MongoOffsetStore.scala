package io.bfil.eventsourcing.mongodb

import scala.concurrent.{ExecutionContext, Future}

import io.bfil.eventsourcing.OffsetStore
import org.mongodb.scala._
import org.mongodb.scala.bson.BsonInt64
import org.mongodb.scala.model._

class MongoOffsetStore(collection: MongoCollection[Document])
                      (implicit executionContext: ExecutionContext) extends OffsetStore {
  collection.createIndex(Indexes.ascending("offsetId")).toFuture()
  def load(offsetId: String): Future[Long] =
    collection.find(Filters.equal("offsetId", offsetId))
              .first()
              .toFuture()
              .map(doc => Option(doc) match {
                case Some(doc) => doc[BsonInt64]("value").getValue()
                case None => 0
              })

  def save(offsetId: String, value: Long): Future[Unit] = {
    val offsetDocument = Document("offsetId" -> offsetId, "value" -> value)
    val updateOptions = new UpdateOptions().upsert(true)
    collection.replaceOne(Filters.equal("offsetId", offsetId), offsetDocument, updateOptions)
              .toFuture()
              .map(completed => ())
  }
}
