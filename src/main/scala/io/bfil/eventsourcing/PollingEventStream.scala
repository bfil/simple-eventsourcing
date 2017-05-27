package io.bfil.eventsourcing

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.util.{Failure, Success}

import util.FutureOps

abstract class PollingEventStream[Event](pollingDelay: FiniteDuration = 1 second)(
  implicit scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  ) extends EventStream[EventEnvelope[Event]] {

  private val streamOffset = new AtomicLong(0)
  private implicit lazy val executionContext = ExecutionContext.fromExecutor(scheduler)

  def poll(offset: Long): Future[Seq[EventEnvelope[Event]]]

  def subscribe(f: EventEnvelope[Event] => Future[Unit], offset: Long = 0): Unit = {
    streamOffset.set(offset)
    if(!scheduler.isShutdown) scheduler.schedule(new PollingTask(f), 0, TimeUnit.SECONDS)
  }

  def shutdown() = {
    scheduler.shutdown()
    scheduler.awaitTermination(10, TimeUnit.SECONDS)
  }

  class PollingTask(f: EventEnvelope[Event] => Future[Unit]) extends Runnable {
    def run() = if(!scheduler.isShutdown) {
      val startOffset = streamOffset.get
      poll(startOffset)
        .flatMap { eventEnvelopes =>
          FutureOps.traverseSequentially(eventEnvelopes) { eventEnvelope =>
            f(eventEnvelope) map { _ =>
              streamOffset.set(eventEnvelope.offset)
            }
          }
        } onComplete { result =>
          val delay = result match {
            case Success(Nil) | Failure(_) => pollingDelay
            case _                         => 0 millis
          }
          scheduler.schedule(new PollingTask(f), delay.toMillis, TimeUnit.MILLISECONDS)
        }
    }
  }

}
