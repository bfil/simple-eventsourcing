package io.bfil.eventsourcing.mongodb

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Await
import scala.concurrent.duration._

import org.mongodb.scala._
import org.mongodb.scala.bson.BsonInt64
import org.mongodb.scala.model._

class MongoJournalOffset(collection: MongoCollection[Document]) {
  private val offset = {
    val lastDocument = Await.result(collection.find()
                                              .sort(Sorts.descending("offset"))
                                              .first()
                                              .toFuture(), 1 second)
    Option(lastDocument) match {
      case Some(doc) => new AtomicLong(doc[BsonInt64]("offset").getValue())
      case None      => new AtomicLong(0)
    }
  }
  def next() = offset.incrementAndGet()
}
