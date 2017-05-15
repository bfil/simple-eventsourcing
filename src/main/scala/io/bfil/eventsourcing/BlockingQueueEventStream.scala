package io.bfil.eventsourcing

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import scala.concurrent.Future

class BlockingQueueEventStream[Event] extends EventStream[Event] {
  private val queue = new LinkedBlockingQueue[Event]()
  def publish(event: Event): Unit = queue.put(event)
  def subscribe(f: Event => Future[Unit], offset: Long = 0): Unit =
    new Thread(new Runnable {
      def run() = while(true) f(queue.take())
    }).start()
}
