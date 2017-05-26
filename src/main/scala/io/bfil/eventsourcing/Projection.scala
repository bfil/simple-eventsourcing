package io.bfil.eventsourcing

import scala.concurrent.Future

abstract class Projection[Event](eventStream: EventStream[Event]) {

  def processEvent(f: Event): Future[Unit]

  def run() = eventStream subscribe processEvent

}
