package io.bfil.eventsourcing

import java.util.concurrent.atomic.AtomicLong

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.control.NonFatal

abstract class PollingEventStream[Event](
  implicit executionContext: ExecutionContext
  ) extends EventStream[EventEnvelope[Event]] {

  private val streamOffset = new AtomicLong(0)

  def poll(offset: Long): Future[Seq[EventEnvelope[Event]]]

  def subscribe(f: EventEnvelope[Event] => Future[Unit], offset: Long = 0): Unit = {
    streamOffset.set(offset)
    new Thread(new Runnable {
      def run() = while(true) {
        val batchResult = poll(streamOffset.get)
                            .flatMap { eventEnvelopes =>
                              traverseSequentially(eventEnvelopes) { eventEnvelope =>
                                if(streamOffset.get == eventEnvelope.offset - 1) {
                                  f(eventEnvelope) map { _ =>
                                    streamOffset.set(eventEnvelope.offset)
                                  }
                                } else Future.failed(
                                  new Exception(s"Unexpected offset: ${eventEnvelope.offset}")
                                )
                              }
                            }
        try {
          Await.result(batchResult, 5 seconds)
        } catch {
          case NonFatal(ex) => ex.printStackTrace
        }
        Thread.sleep(500)
      }
    }).start()
  }

  private def traverseSequentially[A, B, M[X] <: TraversableOnce[X]](in: M[A])(fn: A => Future[B])(implicit cbf: CanBuildFrom[M[A], B, M[B]]): Future[M[B]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      for (r <- fr; b <- fn(a)) yield (r += b)
    }.map(_.result())

}
