package io.bfil.eventsourcing

import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future}

trait PollingEventStream[Event] extends EventStream[EventEnvelope[Event]] {

  private val streamOffset = new AtomicLong(0)
  private implicit lazy val scheduler = Executors.newScheduledThreadPool(2)
  private implicit val executionContext = ExecutionContext.fromExecutor(scheduler)

  class PollingTask(f: EventEnvelope[Event] => Future[Unit]) extends Runnable {
    def run() = {
      val startOffset = streamOffset.get
      poll(startOffset)
        .flatMap { eventEnvelopes =>
          traverseSequentially(eventEnvelopes) { eventEnvelope =>
            f(eventEnvelope) map { _ =>
              streamOffset.set(eventEnvelope.offset)
            }
          }
        } onComplete { _ =>
          if(streamOffset.get == startOffset) scheduler.schedule(new PollingTask(f), 500, TimeUnit.MILLISECONDS)
          else scheduler.schedule(new PollingTask(f), 50, TimeUnit.MILLISECONDS)
        }
    }
  }

  def poll(offset: Long): Future[Seq[EventEnvelope[Event]]]

  def subscribe(f: EventEnvelope[Event] => Future[Unit], offset: Long = 0): Unit = {
    streamOffset.set(offset)
    scheduler.schedule(new PollingTask(f), 0, TimeUnit.SECONDS)
  }

  private def traverseSequentially[A, B, M[X] <: TraversableOnce[X]](in: M[A])(fn: A => Future[B])(implicit cbf: CanBuildFrom[M[A], B, M[B]]): Future[M[B]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      for (r <- fr; b <- fn(a)) yield (r += b)
    }.map(_.result())

}
