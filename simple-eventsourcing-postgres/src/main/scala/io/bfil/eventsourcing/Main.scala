package io.bfil.eventsourcing

import java.util.concurrent.Executors
import javax.sql.DataSource

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import io.circe.generic.auto._
import io.bfil.eventsourcing.circe.JsonEncoding
import io.bfil.eventsourcing.postgres.{PostgresJournal, PostgresOffsetStore, PostgresPollingEventStream}
import io.bfil.eventsourcing.serialization._
import io.bfil.eventsourcing.util.ResourceManagement._

object Main extends App {

  val dataSource: HikariDataSource = {
    val config = new HikariConfig()
    config.setJdbcUrl("jdbc:postgresql://localhost/simple_eventsourcing")
    new HikariDataSource(config)
  }

  withResource(dataSource.getConnection()) { connection =>
    withResource(connection.createStatement()) { statement =>
      statement.execute("""
        DROP TABLE IF EXISTS journal;
        DROP TABLE IF EXISTS offsets;
        DROP TABLE IF EXISTS bank_accounts;
      """)
    }
  }

  implicit val bankAccountEventSerializer = new BankAccountEventSerializer

  val journal = new PostgresJournal[BankAccountEvent](dataSource)

  val aggregatesExecutor = Executors.newSingleThreadExecutor()
  implicit val aggregatesExecutionContext = ExecutionContext.fromExecutor(aggregatesExecutor)

  1 to 100 foreach { id =>
    val bankAccount = new BankAccountAggregate(id, journal)
    for {
      _ <- bankAccount.open("Bruno", 1000)
      _ <- bankAccount.withdraw(100)
      _ <- bankAccount.withdraw(100)
    } yield ()
  }

  val offsetStore = new PostgresOffsetStore(dataSource)
  val journalEventStream = new PostgresPollingEventStream[BankAccountEvent](dataSource)

  val projectionExecutor = Executors.newSingleThreadExecutor()
  val projectionExecutionContext = ExecutionContext.fromExecutor(aggregatesExecutor)
  val bankAccountsProjection = new BankAccountsProjection(dataSource, "bank_accounts", journalEventStream, offsetStore)(projectionExecutionContext)

  val start = System.currentTimeMillis
  bankAccountsProjection.run()
  while (Await.result(offsetStore.load("bank-accounts-projection"), 3 seconds) != 300) {
    Thread.sleep(100)
  }
  println(s"Projection run in ${System.currentTimeMillis - start}ms")

  journal.shutdown()
  offsetStore.shudown()
  journalEventStream.shutdown()

  aggregatesExecutor.shutdown()
  projectionExecutor.shutdown()
}

sealed trait BankAccountState extends AggregateState[BankAccountEvent, BankAccountState]
case object Empty extends BankAccountState {
  val eventHandler = EventHandler {
    case BankAccountOpened(id, name, balance) => BankAccount(id, name, balance)
  }
}
case class BankAccount(id: Int, name: String, balance: Int) extends BankAccountState {
  val eventHandler = EventHandler {
    case MoneyWithdrawn(id, amount) => copy(balance = balance - amount)
  }
}

sealed trait BankAccountEvent
case class BankAccountOpened(id: Int, name: String, balance: Int) extends BankAccountEvent
case class MoneyWithdrawn(id: Int, amount: Int) extends BankAccountEvent

class BankAccountEventSerializer extends EventSerializer[BankAccountEvent] {
  import JsonEncoding._
  def serialize(event: BankAccountEvent) = event match {
    case event: BankAccountOpened => SerializedEvent("BankAccountOpened.V1", encode(event))
    case event: MoneyWithdrawn    => SerializedEvent("MoneyWithdrawn.V1", encode(event))
  }
  def deserialize(manifest: String, data: String) = manifest match {
    case "BankAccountOpened.V1" => decode[BankAccountOpened](data)
    case "MoneyWithdrawn.V1"    => decode[MoneyWithdrawn](data)
  }
}

class BankAccountAggregate(id: Int, journal: JournalWithOptimisticLocking[BankAccountEvent])(implicit executionContext: ExecutionContext)
  extends Aggregate[BankAccountEvent, BankAccountState](journal) {

  val aggregateId = s"bank-account-$id"
  val initialState = Empty

  def onEvent(state: BankAccountState, event: BankAccountEvent): BankAccountState = state.eventHandler(event)

  private def recoverBankAccount(): Future[(BankAccount, VersionedState[BankAccountState])] =
    recover map {
      case state @ VersionedState(bankAccount: BankAccount, _) => (bankAccount, state)
      case _                                                   => throw new Exception(s"Bank account with id '$id' not found")
    }

  def open(name: String, balance: Int): Future[BankAccount] =
    for {
      versionedState <- recover
      bankAccount    <- versionedState.state match {
        case Empty => persist(versionedState, BankAccountOpened(id, name, balance)).mapStateTo[BankAccount]
        case _     => Future.failed(new Exception(s"Bank account with id '$id' already exists"))
      }
    } yield bankAccount

  def withdraw(amount: Int): Future[Int] = retry(1) {
    for {
      (bankAccount, versionedState) <- recoverBankAccount
      updatedBankAccount <-
        if(bankAccount.balance >= amount) {
          persist(versionedState, MoneyWithdrawn(id, amount)).mapStateTo[BankAccount]
        } else Future.failed(new Exception(s"Not enough funds in account with id '$id'"))
    } yield updatedBankAccount.balance
  }
}

class BankAccountsProjection(
  dataSource: DataSource,
  tableName: String,
  eventStream: EventStream[BankAccountEvent],
  offsetStore: OffsetStore
  )(implicit executionContext: ExecutionContext) extends ResumableProjection[BankAccountEvent](eventStream, offsetStore) {

  val projectionId = "bank-accounts-projection"

  withResource(dataSource.getConnection()) { connection =>
    withResource(connection.createStatement()) { statement =>
      statement.execute(s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          id            bigint PRIMARY KEY,
          name          varchar(100),
          balance       integer
        )
      """)
    }
  }

  def processEvent(event: BankAccountEvent): Future[Unit] = event match {
    case BankAccountOpened(id, name, balance) =>
      Future {
        withResource(dataSource.getConnection()) { connection =>
          withResource(connection.prepareStatement(s"INSERT INTO $tableName(id, name, balance) VALUES (?, ?, ?)")) { statement =>
            statement.setLong(1, id)
            statement.setString(2, name)
            statement.setInt(3, balance)
            statement.execute()
          }
        }
      }
    case MoneyWithdrawn(id, amount) =>
      Future {
        withResource(dataSource.getConnection()) { connection =>
          withResource(connection.prepareStatement(s"UPDATE $tableName SET balance = balance - ? WHERE id = ?")) { statement =>
            statement.setInt(1, amount)
            statement.setLong(2, id)
            statement.execute()
          }
        }
      }
  }

}
