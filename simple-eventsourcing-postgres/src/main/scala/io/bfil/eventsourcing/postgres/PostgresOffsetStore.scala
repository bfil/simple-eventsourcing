package io.bfil.eventsourcing.postgres

import java.sql.DriverManager
import java.util.concurrent.Executors

import scala.concurrent.{ExecutionContext, Future}
import io.bfil.eventsourcing.OffsetStore

class PostgresOffsetStore(connectionString: String, tableName: String = "offsets") extends OffsetStore {

  private val connection = DriverManager.getConnection(connectionString)
  private val executor = Executors.newSingleThreadExecutor()
  private implicit val executionContext = ExecutionContext.fromExecutor(executor)

  private val statement = connection.createStatement
  statement.execute(s"""
    CREATE TABLE IF NOT EXISTS $tableName (
      offset_id     varchar(100) PRIMARY KEY,
      value         bigint
    )
  """)
  statement.close()

  def load(offsetId: String): Future[Long] = Future {
    val loadStatement = connection.prepareStatement(s"SELECT * FROM $tableName WHERE offset_id = ?")
    loadStatement.setString(1, offsetId)
    val resultSet = loadStatement.executeQuery()
    val offset = if(resultSet.next()) resultSet.getLong("value") else 0
    resultSet.close()
    loadStatement.close()
    offset
  }

  def save(offsetId: String, value: Long): Future[Unit] = Future {
    val writeStatement = connection.prepareStatement(s"INSERT INTO $tableName(offset_id, value) VALUES (?, ?) ON CONFLICT (offset_id) DO UPDATE SET value = EXCLUDED.value")
    writeStatement.setString(1, offsetId)
    writeStatement.setLong(2, value)
    writeStatement.execute()
    writeStatement.close()
  }

  def shudown() = {
    connection.close()
    executor.shutdown()
  }

}
