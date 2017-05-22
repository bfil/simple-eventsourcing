package io.bfil.eventsourcing

import scala.concurrent.Future

trait JournalPoller[Event] {
  def poll(offset: Long = 0, limit: Int = Int.MaxValue): Future[Seq[Event]]
}
