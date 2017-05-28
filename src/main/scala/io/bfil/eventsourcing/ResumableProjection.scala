package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

abstract class ResumableProjection[Event](
  eventStream: EventStream[Event],
  offsetStore: OffsetStore
  )(implicit executionContext: ExecutionContext) {

  val projectionId: String

  def processEvent(f: Event): Future[Unit]

  def run(): Future[Unit] =
    for {
      lastOffset <- offsetStore.load(projectionId)
    } yield eventStream.subscribe(envelope =>
      for {
        _ <- processEvent(envelope.event)
        _ <- offsetStore.save(projectionId, envelope.offset)
                        .recoverWith(onOffsetSaveError(envelope.event))
      } yield (),
      lastOffset
    )

  def onOffsetSaveError(event: Event): PartialFunction[Throwable, Future[Unit]] = {
    case NonFatal(ex) => Future.failed(ex)
  }

}
