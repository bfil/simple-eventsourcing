package io.bfil.eventsourcing.mongodb

import java.time.{Instant, ZoneOffset}
import java.time.format.DateTimeFormatter

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future}

import io.bfil.eventsourcing.Journal
import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}
import org.mongodb.scala.model._

class MongoJournal[Event](
  collection: MongoCollection[Document]
  )(implicit
    executionContext: ExecutionContext,
    serializer: MongoJournalEventSerializer[Event]
  ) extends Journal[Event] {

  collection.createIndex(Indexes.ascending("offset"), new IndexOptions().unique(true)).toFuture()
  collection.createIndex(Indexes.ascending("aggregateId")).toFuture()

  protected def retryOnDuplicateKeyException[T](f: => Future[T]): Future[T] =
    f recoverWith {
      case ex: com.mongodb.MongoWriteException if ex.getError().getCode() == 11000 => retryOnDuplicateKeyException(f)
    }

  private def nextOffset() = collection.find()
                                       .sort(Sorts.descending("offset"))
                                       .first()
                                       .toFuture()
                                       .map(doc => Option(doc) match {
                                         case Some(doc) => doc[BsonInt64]("offset").getValue() + 1
                                         case None => 1
                                       })

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

  def write(aggregateId: String, events: Seq[Event]): Future[Unit] =
    traverseSequentially(events) { event =>
      val (manifest, data) = serializer.serialize(event)
      retryOnDuplicateKeyException {
        nextOffset() flatMap { offset =>
          val document = Document(
            "offset" -> offset,
            "aggregateId" -> aggregateId,
            "manifest" -> manifest,
            "data" -> Document(data),
            "timestamp" -> Instant.now.atZone(ZoneOffset.UTC)
                                      .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
          )
          collection.insertOne(document)
                    .toFuture()
                    .map(completed => ())
        }
      }
    } map { _ => () }

  private def traverseSequentially[A, B, M[X] <: TraversableOnce[X]](in: M[A])(fn: A => Future[B])(implicit cbf: CanBuildFrom[M[A], B, M[B]]): Future[M[B]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      for (r <- fr; b <- fn(a)) yield (r += b)
    }.map(_.result())
}
