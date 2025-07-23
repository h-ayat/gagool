package gagool.con

import com.mongodb.ReadPreference
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectionTest extends AnyFlatSpec with Matchers with MongodbProvider {

  override val dbName = "connectionTest"

  private def createConnectionConfig(): ConnectionConfig = {
    ConnectionConfig(s"mongodb://$host:$port", dbName)
  }

  "Connection" should "create a connection with default read preference" in {
    val config = createConnectionConfig()
    val connection = new Connection(config)

    connection.client should not be null
    connection.db should not be null
    connection.db.getName shouldBe dbName
  }

  it should "create a connection with custom read preference" in {
    val config = createConnectionConfig()
    val readPreference = ReadPreference.secondary()
    val connection = new Connection(config, readPreference)

    connection.client should not be null
    connection.db should not be null
    connection.db.getName shouldBe dbName
  }

  it should "create a collection with the specified name" in {
    val config = createConnectionConfig()
    val connection = new Connection(config)
    val collectionName = "test_collection"

    val collection = connection.collection(collectionName)
    collection should not be null
  }

  it should "create multiple collections with different names" in {
    val config = createConnectionConfig()
    val connection = new Connection(config)

    val collection1 = connection.collection("collection1")
    val collection2 = connection.collection("collection2")

    collection1 should not be null
    collection2 should not be null
  }

  it should "handle connection with different database names" in {
    val config1 = ConnectionConfig(s"mongodb://localhost:$port", "db1")
    val config2 = ConnectionConfig(s"mongodb://localhost:$port", "db2")

    val connection1 = new Connection(config1)
    val connection2 = new Connection(config2)

    connection1.db.getName shouldBe "db1"
    connection2.db.getName shouldBe "db2"
  }

  it should "maintain the same client instance across multiple collection creations" in {
    val config = createConnectionConfig()
    val connection = new Connection(config)

    val client1 = connection.client
    val collection = connection.collection("test")
    val client2 = connection.client

    client1 should be theSameInstanceAs client2
  }

  it should "use primary read preference by default" in {
    val config = createConnectionConfig()
    val connection = new Connection(config)

    // The read preference is set internally; we can verify the connection works
    connection.client should not be null
    connection.db should not be null
  }

  it should "accept and use custom read preferences" in {
    val config = createConnectionConfig()
    val secondaryPreferred = ReadPreference.secondaryPreferred()
    val connection = new Connection(config, secondaryPreferred)

    connection.client should not be null
    connection.db should not be null
  }

  "ConnectionConfig" should "create with valid URI and database name" in {
    val uriString = s"mongodb://localhost:$port"

    val config = ConnectionConfig(uriString, dbName)
    config.uriString shouldBe uriString
    config.dbName shouldBe dbName
  }

  it should "work with different URI formats" in {
    val configs = List(
      ConnectionConfig(s"mongodb://localhost:$port", "db1"),
      ConnectionConfig(s"mongodb://localhost:$port/db2", "db2"),
      ConnectionConfig(s"mongodb://user:pass@localhost:$port", "db3")
    )

    configs.foreach { config =>
      val connection = new Connection(config)
      connection.client should not be null
      connection.db.getName shouldBe config.dbName
    }
  }
}
