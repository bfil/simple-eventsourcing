package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class ResumableProjection[Event](implicit executionContext: ExecutionContext) {
  self: EventStreamProvider[Event] with OffsetStoreProvider =>

  val projectionId: String

  def processEvent(f: Event): Future[Unit]
  
  def getEventOffset(event: Event): Long

  def run() =
    for {
      lastOffset <- offsetStore.read(projectionId)
    } yield eventStream.subscribe(
      event => {
        val eventOffset = getEventOffset(event)
        if(eventOffset > lastOffset) {
          for {
            _ <- processEvent(event)
            _ <- offsetStore.write(projectionId, eventOffset)
          } yield ()
        } else Future.successful(())
      },
      lastOffset
    )

}
