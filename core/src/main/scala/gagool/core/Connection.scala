package gagool.core

import com.mongodb.*
import com.mongodb.reactivestreams.client.*
import org.bson.BsonDocument
import cats.effect.Async

case class ConnectionConfig(uriString: String, dbName: String)

class Connection[E[_]: Async](
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

object Tester {
  import cats.effect.Async
  import org.reactivestreams.Publisher
  import fs2.interop.reactivestreams.fromPublisher

  def fromPublisher[F[_]: Async, A](pub: Publisher[A]): F[List[A]] =
    val o = fromPublisher[F, A](pub)
    o

}
