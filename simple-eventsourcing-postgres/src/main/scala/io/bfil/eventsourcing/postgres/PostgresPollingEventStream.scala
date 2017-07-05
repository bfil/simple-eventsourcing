package io.bfil.eventsourcing.postgres

import java.sql.Connection
import java.time.Instant

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import io.bfil.eventsourcing.{EventEnvelope, PollingEventStream}
import io.bfil.eventsourcing.serialization.EventSerializer

class PostgresPollingEventStream[Event](connection: Connection)(implicit executionContext: ExecutionContext, serializer: EventSerializer[Event])
  extends PollingEventStream[Event](500 millis) {

  def poll(offset: Long): Future[Seq[EventEnvelope[Event]]] = Future {
    val statement = connection.createStatement
    val resultSet = statement.executeQuery(s"""SELECT * FROM journal WHERE "offset" > $offset LIMIT 100""")
    var events = Seq.empty[EventEnvelope[Event]]
    while(resultSet.next()) {
      val offset          = resultSet.getLong("offset")
      val aggregateId     = resultSet.getString("aggregate_id")
      val manifest        = resultSet.getString("manifest")
      val data            = resultSet.getString("data")
      val timestamp       = resultSet.getTimestamp("timestamp")
      val event           = serializer.deserialize(manifest, data)
      events = events :+ EventEnvelope(offset, aggregateId, event, Instant.ofEpochMilli(timestamp.getTime))
    }
    resultSet.close()
    statement.close()
    events
  }

}
