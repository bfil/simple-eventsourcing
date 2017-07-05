package io.bfil.eventsourcing

import java.sql._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import io.circe.generic.auto._
import io.bfil.eventsourcing.circe.JsonEncoding
import io.bfil.eventsourcing.postgres.{PostgresJournal, PostgresOffsetStore, PostgresPollingEventStream}
import io.bfil.eventsourcing.serialization._

object Main extends App {
  Class.forName("org.postgresql.Driver")

  val connection: Connection = DriverManager.getConnection("jdbc:postgresql://localhost/simple_eventsourcing")

  implicit val bankAccountEventSerializer = new BankAccountEventSerializer

  val statement = connection.createStatement
  statement.execute("""
    DROP TABLE IF EXISTS journal;
    DROP TABLE IF EXISTS offsets;
    DROP TABLE IF EXISTS bank_accounts;
  """)
  statement.close()

  val journal = new PostgresJournal[BankAccountEvent](connection)

  1 to 100 foreach { id =>
    val bankAccount = new BankAccountAggregate(id, journal)
    for {
      _ <- bankAccount.open("Bruno", 1000)
      _ <- bankAccount.withdraw(100)
      _ <- bankAccount.withdraw(100)
    } yield ()
  }

  val offsetStore = new PostgresOffsetStore(connection)
  val journalEventStream = new PostgresPollingEventStream[BankAccountEvent](connection)

  val bankAccounts = new BankAccountsProjection(connection, journalEventStream, offsetStore)

  val start = System.currentTimeMillis
  bankAccounts.run()
  while (Await.result(offsetStore.load("bank-accounts-projection"), 3 second) != 300) {
    Thread.sleep(100)
  }
  println(s"Projection run in ${System.currentTimeMillis - start}ms")

  journalEventStream.shutdown()
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

class BankAccountAggregate(id: Int, journal: JournalWithOptimisticLocking[BankAccountEvent])
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
  connection: Connection,
  eventStream: EventStream[BankAccountEvent],
  offsetStore: OffsetStore
  ) extends ResumableProjection[BankAccountEvent](eventStream, offsetStore) {
  val projectionId = "bank-accounts-projection"

  val statement = connection.createStatement
  statement.execute("""
    CREATE TABLE IF NOT EXISTS bank_accounts (
      id            bigint PRIMARY KEY,
      name          varchar(100),
      balance       integer
    )
  """)
  statement.close()

  def processEvent(event: BankAccountEvent): Future[Unit] = event match {
    case BankAccountOpened(id, name, balance) =>
      Future {
        val query = "INSERT INTO bank_accounts(id, name, balance) VALUES (?, ?, ?)"
        val preparedStatement = connection.prepareStatement(query)
        preparedStatement.setLong(1, id)
        preparedStatement.setString(2, name)
        preparedStatement.setInt(3, balance)
        preparedStatement.execute()
        preparedStatement.close()
      }
    case MoneyWithdrawn(id, amount) =>
      Future {
        val query = "UPDATE bank_accounts SET balance = balance - ? WHERE id = ?"
        val preparedStatement = connection.prepareStatement(query)
        preparedStatement.setInt(1, amount)
        preparedStatement.setLong(2, id)
        preparedStatement.execute()
        preparedStatement.close()
      }
  }
}
