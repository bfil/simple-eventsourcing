package io.bfil.eventsourcing.mongodb

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

import io.bfil.eventsourcing.{EventEnvelope, EventStream}
import org.mongodb.scala._
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}
import org.mongodb.scala.model._

class MongoJournalEventStream[Event](
  collection: MongoCollection[Document]
  )(implicit
    executionContext: ExecutionContext,
    serializer: MongoJournalEventSerializer[Event]
  ) extends EventStream[EventEnvelope[Event]] {

  private val streamOffset = new AtomicLong(0)

  def subscribe(f: EventEnvelope[Event] => Future[Unit], offset: Long = 0): Unit = {
    streamOffset.set(offset)
    new Thread(new Runnable {
      def run() = while(true) {
        val batchResult = collection.find(Filters.gt("offset", streamOffset.get))
                                    .sort(Sorts.ascending("offset"))
                                    .limit(100)
                                    .toFuture()
                                    .flatMap { docs =>
                                      traverseSequentially(docs) { doc =>
                                        val eventEnvelope = documentToEventEnvelope(doc)
                                        f(eventEnvelope) map { _ =>
                                          streamOffset.set(eventEnvelope.offset)
                                        }
                                      }
                                    }
        try {
          Await.result(batchResult, 1 second)
        } catch {
          case NonFatal(ex) => // Just retry
        }
        Thread.sleep(1000)
      }
    }).start()
  }

  private def traverseSequentially[A, B, M[X] <: TraversableOnce[X]](in: M[A])(fn: A => Future[B])(implicit cbf: CanBuildFrom[M[A], B, M[B]]): Future[M[B]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      for (r <- fr; b <- fn(a)) yield (r += b)
    }.map(_.result())

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
