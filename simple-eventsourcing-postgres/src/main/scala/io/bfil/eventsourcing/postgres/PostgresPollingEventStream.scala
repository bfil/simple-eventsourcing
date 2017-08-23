package io.bfil.eventsourcing.postgres

import java.time.Instant
import javax.sql.DataSource

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

import io.bfil.eventsourcing.{EventEnvelope, PollingEventStream}
import io.bfil.eventsourcing.serialization.EventSerializer
import io.bfil.eventsourcing.util.ResourceManagement._

class PostgresPollingEventStream[Event](dataSource: DataSource, pollSize: Int = 1000)(implicit serializer: EventSerializer[Event])
  extends PollingEventStream[Event](500 millis) {

  def poll(offset: Long): Future[Seq[EventEnvelope[Event]]] = Future {
    withResource(dataSource.getConnection()) { connection =>
      withResource(connection.prepareStatement(s"""SELECT * FROM journal WHERE "offset" > ? ORDER BY "offset" LIMIT $pollSize""")) { statement =>
        statement.setLong(1, offset)
        withResource(statement.executeQuery()) { resultSet =>
          var events = Seq.empty[EventEnvelope[Event]]
          while (resultSet.next()) {
            val offset = resultSet.getLong("offset")
            val aggregateId = resultSet.getString("aggregate_id")
            val manifest = resultSet.getString("manifest")
            val data = resultSet.getString("data")
            val timestamp = resultSet.getTimestamp("timestamp")
            val event = serializer.deserialize(manifest, data)
            events = events :+ EventEnvelope(offset, aggregateId, event, Instant.ofEpochMilli(timestamp.getTime))
          }
          events
        }
      }
    }
  }

}
