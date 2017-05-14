package io.bfil.eventsourcing

import java.util.concurrent.BlockingQueue

import scala.concurrent.Future

class BlockingQueueEventStream[Event](queue: BlockingQueue[Event]) extends EventStream[Event] {
  def subscribe(f: Event => Future[Unit], offset: Long = 0): Unit =
    new Thread(new Runnable {
      def run() = while(true) f(queue.take())
    }).start()
}
