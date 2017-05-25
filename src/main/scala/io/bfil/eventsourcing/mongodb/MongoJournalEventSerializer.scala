package io.bfil.eventsourcing.mongodb

import io.circe._
import io.circe.parser._
import io.circe.syntax._

trait MongoJournalEventSerializer[Event] {
  def serialize(event: Event): (String, String)
  def deserialize(manifest: String, data: String): Event

  protected def toJson[Event: Encoder](event: Event): String = event.asJson.noSpaces
  protected def fromJson[Event: Decoder](data: String): Event = decode[Event](data) match {
    case Right(event) => event
    case Left(error)  => throw new Exception(s"Unable to decode event: $error")
  }
}
