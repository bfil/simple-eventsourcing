package io.bfil.eventsourcing.mongodb

import java.lang.InterruptedException
import java.util.concurrent.{Executors, ExecutorService, LinkedBlockingQueue, TimeUnit}

import scala.concurrent.{ExecutionContext, Future, Promise}

import org.mongodb.scala._
import org.mongodb.scala.bson.BsonInt64
import org.mongodb.scala.model._

class MongoJournalWriter(collection: MongoCollection[Document])
                        (implicit executor: ExecutorService = Executors.newSingleThreadExecutor()) {

  private implicit val executionContext = ExecutionContext.fromExecutor(executor)

  private val queue = new LinkedBlockingQueue[(Document, Promise[Unit])]()
  executor.execute(new WriteNextEventTask())

  private def nextOffset() = collection.find()
                                       .sort(Sorts.descending("offset"))
                                       .first()
                                       .toFuture()
                                       .map(doc => Option(doc) match {
                                         case Some(doc) => doc[BsonInt64]("offset").getValue() + 1
                                         case None => 1
                                       })

  def write(document: Document): Future[Unit] = {
    val p = Promise[Unit]()
    queue.put((document, p))
    p.future
  }

  def shutdown() = {
    executor.shutdownNow()
    executor.awaitTermination(10, TimeUnit.SECONDS)
  }

  private class WriteNextEventTask() extends Runnable {
    def run() = if(!executor.isShutdown) {
      try {
        val (document, promise) = queue.take()
        try {
          nextOffset() foreach { offset =>
            collection.insertOne(document ++ Document("offset" -> offset))
                      .toFuture()
                      .map(completed => ()) onComplete { result =>
              promise complete result
              executor execute new WriteNextEventTask()
            }
          }
        } catch {
          case ex: InterruptedException => promise failure ex
        }
      } catch {
        case ex: InterruptedException => ()
      }
    }
  }

}
