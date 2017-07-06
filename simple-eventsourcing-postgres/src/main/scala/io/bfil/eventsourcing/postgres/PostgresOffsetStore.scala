package io.bfil.eventsourcing.postgres

import java.util.concurrent.Executors
import javax.sql.DataSource

import scala.concurrent.{ExecutionContext, Future}

import io.bfil.eventsourcing.OffsetStore
import io.bfil.eventsourcing.util.ResourceManagement._

class PostgresOffsetStore(dataSource: DataSource, tableName: String = "offsets") extends OffsetStore {

  private val executor = Executors.newSingleThreadExecutor()
  private implicit val executionContext = ExecutionContext.fromExecutor(executor)

  withResource(dataSource.getConnection()) { connection =>
    withResource(connection.createStatement()) { statement =>
      statement.execute(s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          offset_id     varchar(100) PRIMARY KEY,
          value         bigint
        )
      """)
    }
  }

  def load(offsetId: String): Future[Long] = Future {
    withResource(dataSource.getConnection()) { connection =>
      withResource(connection.prepareStatement(s"SELECT * FROM $tableName WHERE offset_id = ?")) { statement =>
        statement.setString(1, offsetId)
        withResource(statement.executeQuery()) { resultSet =>
          if (resultSet.next()) resultSet.getLong("value") else 0
        }
      }
    }
  }

  def save(offsetId: String, value: Long): Future[Unit] = Future {
    withResource(dataSource.getConnection()) { connection =>
      withResource(connection.prepareStatement(s"INSERT INTO $tableName(offset_id, value) VALUES (?, ?) ON CONFLICT (offset_id) DO UPDATE SET value = EXCLUDED.value")) { statement =>
        statement.setString(1, offsetId)
        statement.setLong(2, value)
        statement.execute()
      }
    }
  }

  def shudown() = {
    executor.shutdown()
  }

}
