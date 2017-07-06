package io.bfil.eventsourcing.postgres

import java.time.Instant
import java.util.concurrent.Executors
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import io.bfil.eventsourcing.{EventEnvelope, PollingEventStream}
import io.bfil.eventsourcing.serialization.EventSerializer

class PostgresPollingEventStream[Event](dataSource: DataSource, pollSize: Int = 1000)(implicit serializer: EventSerializer[Event])
  extends PollingEventStream[Event](500 millis) {

  private val executor = Executors.newSingleThreadExecutor()
  private implicit val executionContext = ExecutionContext.fromExecutor(executor)

  def poll(offset: Long): Future[Seq[EventEnvelope[Event]]] = Future {
    val connection = dataSource.getConnection()
    val pollStatement = connection.prepareStatement(s"""SELECT * FROM journal WHERE "offset" > ? LIMIT $pollSize""")
    pollStatement.setLong(1, offset)
    val resultSet = pollStatement.executeQuery()
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
    pollStatement.close()
    connection.close()
    events
  }

  override def shutdown() = {
    executor.shutdown()
    super.shutdown()
  }

}