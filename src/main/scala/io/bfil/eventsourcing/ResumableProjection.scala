package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

abstract class ResumableProjection[Event](
  eventStream: EventStream[Event], offsetStore: OffsetStore
  )(implicit executionContext: ExecutionContext) {

  val projectionId: String

  def processEvent(f: Event): Future[Unit]

  def getEventOffset(event: Event): Long

  def onOffsetSaveError(event: Event): PartialFunction[Throwable, Future[Unit]] = {
    case NonFatal(ex) => Future.failed(ex)
  }

  def run(): Future[Unit] =
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
