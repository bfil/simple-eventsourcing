package io.bfil.eventsourcing.inmemory

import java.util.concurrent.LinkedBlockingQueue

import scala.concurrent.Future

import io.bfil.eventsourcing.EventStream

class InMemoryEventStream[Event] extends EventStream[Event] {
  private val queue = new LinkedBlockingQueue[Event]()
  def publish(event: Event): Unit = queue.put(event)
  def subscribe(f: Event => Future[Unit], offset: Long = 0): Unit =
    new Thread(new Runnable {
      def run() = while(true) f(queue.take())
    }).start()
}
