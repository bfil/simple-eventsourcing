package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}

abstract class ResumableProjection[Event](implicit executionContext: ExecutionContext) {
  self: EventStreamProvider[Event] with OffsetStoreProvider =>

  val projectionId: String

  def processEvent(f: Event): Future[Unit]

  def getEventOffset(event: Event): Long

  def onOffsetSaveError(event: Event): PartialFunction[Throwable, Future[Unit]]

  def run() =
    for {
      lastOffset <- offsetStore.load(projectionId)
    } yield eventStream.subscribe(
      event => {
        val eventOffset = getEventOffset(event)
        if(eventOffset > lastOffset) {
          for {
            _ <- processEvent(event)
            _ <- offsetStore.save(projectionId, eventOffset)
                            .recoverWith(onOffsetSaveError(event))
          } yield ()
        } else Future.successful(())
      },
      lastOffset
    )

}
