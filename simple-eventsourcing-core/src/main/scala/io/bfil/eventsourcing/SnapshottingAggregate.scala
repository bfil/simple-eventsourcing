package io.bfil.eventsourcing

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

abstract class SnapshottingAggregate[Event, State](
  journal: JournalWithOptimisticLocking[Event],
  snapshotStore: SnapshotStore[State],
  snapshotInterval: Long = 100
  )(implicit executionContext: ExecutionContext) extends Aggregate[Event, State](journal) {

  override def recover(): Future[VersionedState[State]] =
    for {
      snapshot      <- snapshotStore.load(aggregateId)
      state          = snapshot.map(_.state).getOrElse(initialState)
      offset         = snapshot.map(_.offset).getOrElse(0L)
      events        <- journal.read(aggregateId, offset)
      versionedState = events.foldLeft(VersionedState(state, offset)) { case (versionedState, eventWithOffset) =>
        VersionedState(onEvent(versionedState.state, eventWithOffset.event), eventWithOffset.offset)
      }
    } yield versionedState

  override def persist(currentVersionedState: VersionedState[State], events: Event*): Future[State] =
    for {
      _         <- journal.write(aggregateId, currentVersionedState.version, events)
      newState   = events.foldLeft(currentVersionedState.state)(onEvent)
      lastOffset = currentVersionedState.version + events.length
      _         <- lastOffset % snapshotInterval match {
        case 0 => snapshotStore.save(aggregateId, Snapshot(newState, lastOffset))
                               .recover { case NonFatal(_) => Future.successful(()) }
        case _ => Future.successful(())
      }
    } yield newState

}
