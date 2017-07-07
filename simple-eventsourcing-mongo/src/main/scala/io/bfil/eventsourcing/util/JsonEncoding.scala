package io.bfil.eventsourcing.util

import io.circe._
import io.circe.syntax._

object JsonEncoding {
  def encode[T: Encoder](value: T): String = value.asJson.noSpaces
  def decode[T: Decoder](json: String): T = parser.decode[T](json) match {
    case Right(value) => value
    case Left(error)  => throw new Exception(s"Unable to decode event: $error")
  }
}
