package io.bfil.eventsourcing.postgres

import java.sql.Connection

import scala.concurrent.{ExecutionContext, Future}
import io.bfil.eventsourcing.OffsetStore

class PostgresOffsetStore(connection: Connection)(implicit executionContext: ExecutionContext) extends OffsetStore {

  val statement = connection.createStatement
  statement.execute("""
    CREATE TABLE IF NOT EXISTS offsets (
      offset_id     varchar(100) PRIMARY KEY,
      value         bigint
    )
  """)
  statement.close()

  def load(offsetId: String): Future[Long] = Future {
    val statement = connection.createStatement
    val resultSet = statement.executeQuery(s"SELECT * FROM offsets WHERE offset_id = '$offsetId'")
    val offset = if(resultSet.next()) resultSet.getLong("value") else 0
    resultSet.close()
    statement.close()
    offset
  }

  def save(offsetId: String, value: Long): Future[Unit] = Future {
    val query = "INSERT INTO offsets(offset_id, value) VALUES (?, ?) ON CONFLICT (offset_id) DO UPDATE SET value = EXCLUDED.value"
    val preparedStatement = connection.prepareStatement(query)
    preparedStatement.setString(1, offsetId)
    preparedStatement.setLong(2, value)
    preparedStatement.execute()
    preparedStatement.close()
  }
}
