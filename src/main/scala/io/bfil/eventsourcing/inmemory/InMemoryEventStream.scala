package io.bfil.eventsourcing.inmemory

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.Future

import io.bfil.eventsourcing.EventStream

class InMemoryEventStream[Event] extends EventStream[Event] {
  private val offset = new AtomicLong(0)
  private val queue = new LinkedBlockingQueue[(Event, Long)]()
  def publish(event: Event): Unit = queue.put((event, offset.incrementAndGet()))
  def subscribe(f: ((Event, Long)) => Future[Unit], offset: Long = 0): Unit =
    new Thread(new Runnable {
      def run() = while(true) f(queue.take())
    }).start()
}
