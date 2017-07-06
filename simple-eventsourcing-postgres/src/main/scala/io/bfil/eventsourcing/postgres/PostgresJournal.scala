package io.bfil.eventsourcing.postgres

import java.sql.DriverManager
import java.util.concurrent.Executors
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import io.bfil.eventsourcing.{EventWithOffset, JournalWithOptimisticLocking}
import io.bfil.eventsourcing.serialization.EventSerializer

class PostgresJournal[Event](dataSource: DataSource, tableName: String = "journal")(implicit serializer: EventSerializer[Event])
  extends JournalWithOptimisticLocking[Event] {

  private val executor = Executors.newSingleThreadExecutor()
  private implicit val executionContext = ExecutionContext.fromExecutor(executor)

  private val connection = dataSource.getConnection()
  private val statement = connection.createStatement
  statement.execute(s"""
    CREATE TABLE IF NOT EXISTS $tableName (
       "offset"          serial PRIMARY KEY,
       aggregate_id      varchar(100),
       aggregate_offset  bigint,
       manifest          varchar(100),
       data              text,
       timestamp         timestamp without time zone DEFAULT(NOW()),
       CONSTRAINT aggregate_versioning UNIQUE(aggregate_id, aggregate_offset)
    )
  """)
  statement.close()
  connection.close()

  def read(aggregateId: String, offset: Long = 0): Future[Seq[EventWithOffset[Event]]] = Future {
    val connection = dataSource.getConnection()
    val readStatement = connection.prepareStatement(s"SELECT * FROM $tableName WHERE aggregate_id = ? AND aggregate_offset > ?")
    readStatement.setString(1, aggregateId)
    readStatement.setLong(2, offset)
    val resultSet = readStatement.executeQuery()
    var events = Seq.empty[EventWithOffset[Event]]
    while(resultSet.next()) {
      val aggregateOffset = resultSet.getLong("aggregate_offset")
      val manifest        = resultSet.getString("manifest")
      val data            = resultSet.getString("data")
      val event           = serializer.deserialize(manifest, data)
      events = events :+ EventWithOffset(event, aggregateOffset)
    }
    resultSet.close()
    readStatement.close()
    connection.close()
    events
  }

  def write(aggregateId: String, lastSeenOffset: Long, events: Seq[Event]): Future[Unit] = Future {
    val connection = dataSource.getConnection()
    val writeStatement = connection.prepareStatement(s"INSERT INTO $tableName(aggregate_id, aggregate_offset, manifest, data) VALUES (?, ?, ?, ?)")
    for ((event, index) <- events.zipWithIndex) {
      val serializedEvent = serializer.serialize(event)
      writeStatement.setString(1, aggregateId)
      writeStatement.setLong(2, lastSeenOffset + index + 1)
      writeStatement.setString(3, serializedEvent.manifest)
      writeStatement.setString(4, serializedEvent.data)
      writeStatement.addBatch()
    }
    writeStatement.executeBatch()
    writeStatement.close()
    connection.close()
  }

  def shutdown() = {
    executor.shutdown()
  }

}
