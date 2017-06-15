package io.bfil.eventsourcing.inmemory

import java.util.concurrent.LinkedBlockingQueue

import scala.concurrent.Future

import io.bfil.eventsourcing.{EventEnvelope, EventStream}

class InMemoryEventStream[Event] extends EventStream[Event] {
  private val queue = new LinkedBlockingQueue[EventEnvelope[Event]]()
  def publish(event: EventEnvelope[Event]): Unit = queue.put(event)
  def subscribe(f: EventEnvelope[Event] => Future[Unit], offset: Long = 0): Unit =
    new Thread(new Runnable {
      def run() = while(true) {
        val envelope = queue.take()
        if (envelope.offset > offset) f(envelope)
      }
    }).start()
}
