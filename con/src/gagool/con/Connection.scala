package gagool.con

import com.mongodb.*
import com.mongodb.reactivestreams.client.*
import org.bson.BsonDocument

case class ConnectionConfig(uriString: String, dbName: String)

class Connection(
    config: ConnectionConfig,
    readPreference: ReadPreference = ReadPreference.primary()
) {

  private val serverApi =
    ServerApi.builder().version(ServerApiVersion.V1).build()
  private val settings = MongoClientSettings
    .builder()
    .applyConnectionString(new ConnectionString(config.uriString))
    .readPreference(readPreference)
    .serverApi(serverApi)
    .build()

  val client: MongoClient = MongoClients.create(settings)
  val db: MongoDatabase = client.getDatabase(config.dbName)

  def collection(name: String) =
    new DocCollection(
      db.getCollection[BsonDocument](name, classOf[BsonDocument]),
      readPreference
    )
}
