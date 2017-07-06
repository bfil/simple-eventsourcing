package io.bfil.eventsourcing.postgres

import java.sql.SQLException
import java.util.concurrent.Executors
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

import io.bfil.eventsourcing.{EventWithOffset, JournalWithOptimisticLocking, OptimisticLockException}
import io.bfil.eventsourcing.serialization.EventSerializer
import io.bfil.eventsourcing.util.TryWith

class PostgresJournal[Event](dataSource: DataSource, tableName: String = "journal")(implicit serializer: EventSerializer[Event])
  extends JournalWithOptimisticLocking[Event] {

  private val executor = Executors.newSingleThreadExecutor()
  private implicit val executionContext = ExecutionContext.fromExecutor(executor)

  TryWith(dataSource.getConnection()) { connection =>
    TryWith(connection.createStatement()) { statement =>
      statement.execute(s"""
        CREATE TABLE IF NOT EXISTS $tableName (
           "offset"          serial PRIMARY KEY,
           aggregate_id      varchar(100),
           aggregate_offset  bigint,
           manifest          varchar(100),
           data              jsonb,
           timestamp         timestamp without time zone DEFAULT(NOW()),
           CONSTRAINT aggregate_versioning UNIQUE(aggregate_id, aggregate_offset)
        )
      """)
    }
  }

  def read(aggregateId: String, offset: Long = 0): Future[Seq[EventWithOffset[Event]]] = Future {
    TryWith(dataSource.getConnection()) { connection =>
      TryWith(connection.prepareStatement(s"SELECT * FROM $tableName WHERE aggregate_id = ? AND aggregate_offset > ?")) { statement =>
        statement.setString(1, aggregateId)
        statement.setLong(2, offset)
        TryWith(statement.executeQuery()) { resultSet =>
          var events = Seq.empty[EventWithOffset[Event]]
          while(resultSet.next()) {
            val aggregateOffset = resultSet.getLong("aggregate_offset")
            val manifest        = resultSet.getString("manifest")
            val data            = resultSet.getString("data")
            val event           = serializer.deserialize(manifest, data)
            events = events :+ EventWithOffset(event, aggregateOffset)
          }
          events
        }
      }
    }
  }

  def write(aggregateId: String, lastSeenOffset: Long, events: Seq[Event]): Future[Unit] = Future {
    TryWith(dataSource.getConnection()) { connection =>
      TryWith(connection.prepareStatement(s"INSERT INTO $tableName(aggregate_id, aggregate_offset, manifest, data) VALUES (?, ?, ?, TO_JSON(?::json))")) { statement =>
        for ((event, index) <- events.zipWithIndex) {
          val serializedEvent = serializer.serialize(event)
          statement.setString(1, aggregateId)
          statement.setLong(2, lastSeenOffset + index + 1)
          statement.setString(3, serializedEvent.manifest)
          statement.setString(4, serializedEvent.data)
          statement.addBatch()
        }
        try {
          statement.executeBatch()
        } catch {
          case ex: SQLException if ex.getSQLState == "23505" => throw new OptimisticLockException(s"Unexpected version $lastSeenOffset for aggregate '$aggregateId")
          case NonFatal(ex) => throw ex
        }
      }
    }
  }

  def shutdown() = {
    executor.shutdown()
  }

}
