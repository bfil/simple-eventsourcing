package io.bfil.eventsourcing.postgres

import java.sql.Connection

import scala.concurrent.{ExecutionContext, Future}
import io.bfil.eventsourcing.{EventWithOffset, JournalWithOptimisticLocking}
import io.bfil.eventsourcing.serialization.EventSerializer

class PostgresJournal[Event](connection: Connection)(implicit executionContext: ExecutionContext, serializer: EventSerializer[Event])
  extends JournalWithOptimisticLocking[Event] {

  val statement = connection.createStatement
  statement.execute("""
    CREATE TABLE IF NOT EXISTS journal (
       "offset"         serial PRIMARY KEY,
       aggregate_id     varchar(100),
       aggregate_offset bigint,
       manifest         varchar(100),
       data             text,
       timestamp        timestamp without time zone DEFAULT(NOW()),
       CONSTRAINT aggregate_versioning UNIQUE(aggregate_id, aggregate_offset)
    )
  """)
  statement.close()

  def read(aggregateId: String, offset: Long = 0): Future[Seq[EventWithOffset[Event]]] = Future {
    val statement = connection.createStatement
    val resultSet = statement.executeQuery(s"SELECT * FROM journal WHERE aggregate_id = '$aggregateId' AND aggregate_offset > $offset")
    var events = Seq.empty[EventWithOffset[Event]]
    while(resultSet.next()) {
      val aggregateOffset = resultSet.getLong("aggregate_offset")
      val manifest        = resultSet.getString("manifest")
      val data            = resultSet.getString("data")
      val event           = serializer.deserialize(manifest, data)
      events = events :+ EventWithOffset(event, aggregateOffset)
    }
    resultSet.close()
    statement.close()
    events
  }

  def write(aggregateId: String, lastSeenOffset: Long, events: Seq[Event]): Future[Unit] = Future {
    val query = "INSERT INTO journal(aggregate_id, aggregate_offset, manifest, data) VALUES (?, ?, ?, ?)"
    val preparedStatement = connection.prepareStatement(query)
    for ((event, index) <- events.zipWithIndex) {
      val serializedEvent = serializer.serialize(event)
      preparedStatement.setString(1, aggregateId)
      preparedStatement.setLong(2, lastSeenOffset + index + 1)
      preparedStatement.setString(3, serializedEvent.manifest)
      preparedStatement.setString(4, serializedEvent.data)
      preparedStatement.addBatch()
    }
    preparedStatement.executeBatch()
    preparedStatement.close()
  }

}
