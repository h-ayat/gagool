package gagool.core

import kyo.*
import org.reactivestreams.{Publisher, Subscriber, Subscription}
import com.mongodb.ReadPreference
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.{
  DeleteResult,
  InsertManyResult,
  InsertOneResult,
  UpdateResult
}
import com.mongodb.reactivestreams.client.{FindPublisher, MongoCollection}
import gagool.bson.{BsonDocDecoder, BsonDocEncoder}
import org.bson.BsonDocument
import org.bson.conversions.Bson
import scala.concurrent.Promise
import scala.jdk.CollectionConverters.*

private object ReactiveUtil:

  import kyo.interop.reactivestreams.*
  import org.reactivestreams.Publisher

  def optional[T: Tag](pub: Publisher[T]): Maybe[T] < Async =
    val effect: Maybe[T] < (Scope & Async) =
      for
        subscriber <- fromPublisher(pub, bufferSize = 1)
        chunk <- subscriber.take(1).run
      yield chunk.headMaybe

    Scope.run(effect)

  def single[T: Tag](pub: Publisher[T]): T < Async =
    val effect: T < (Scope & Async) =
      for
        subscriber <- fromPublisher(pub, bufferSize = 1)
        chunk <- subscriber.take(1).run
      yield chunk.head

    Scope.run(effect)

  def drain[T: Tag](pub: Publisher[T]): Unit < Async =
    Scope.run(fromPublisher(pub, bufferSize = 1).map(_ => ()))

  def collect[T: Tag](pub: Publisher[T]): List[T] < Async =
    val effect: List[T] < (Scope & Async) =
      for
        subscriber <- fromPublisher(pub, bufferSize = 100)
        chunk <- subscriber.run
      yield chunk.toList
    Scope.run(effect)

class DocCollection(
    val base: MongoCollection[BsonDocument],
    readPreference: ReadPreference
):

  def find[F: BsonDocEncoder, P: BsonDocEncoder, O: BsonDocEncoder](
      filter: F,
      projection: Option[P] = None,
      order: Option[O] = None,
      options: FinderOptions = FinderOptions.default
  ): Finder =
    Finder(
      summon[BsonDocEncoder[F]].encode(filter),
      projection.map(summon[BsonDocEncoder[P]].encode),
      order.map(summon[BsonDocEncoder[O]].encode),
      options,
      this
    )

  def insert[T: BsonDocEncoder](doc: T): InsertOneResult < Async =
    ReactiveUtil.single(base.insertOne(summon[BsonDocEncoder[T]].encode(doc)))

  def insertAll[T: BsonDocEncoder](docs: List[T]): InsertManyResult < Async =
    val bsonDocs = docs.map(summon[BsonDocEncoder[T]].encode).asJava
    ReactiveUtil.single(base.insertMany(bsonDocs))

  def updateOne[F: BsonDocEncoder, U: BsonDocEncoder](
      filter: F,
      update: U,
      upsert: Boolean = false
  ): UpdateResult < Async =
    val opts = new UpdateOptions().upsert(upsert)
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    val u: Bson = summon[BsonDocEncoder[U]].encode(update)
    ReactiveUtil.single(base.updateOne(f, u, opts))

  def updateMany[F: BsonDocEncoder, U: BsonDocEncoder](
      filter: F,
      update: U,
      upsert: Boolean = false
  ): UpdateResult < Async =
    val opts = new UpdateOptions().upsert(upsert)
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    val u: Bson = summon[BsonDocEncoder[U]].encode(update)
    ReactiveUtil.single(base.updateMany(f, u, opts))

  def deleteOne[F: BsonDocEncoder](filter: F): DeleteResult < Async =
    ReactiveUtil.single(
      base.deleteOne(summon[BsonDocEncoder[F]].encode(filter))
    )

  def deleteMany[F: BsonDocEncoder](filter: F): DeleteResult < Async =
    ReactiveUtil.single(
      base.deleteMany(summon[BsonDocEncoder[F]].encode(filter))
    )

  def createIndex[K: BsonDocEncoder](
      keys: K,
      options: IndexOptions = new IndexOptions()
  ): String < Async =
    ReactiveUtil.single(
      base.createIndex(summon[BsonDocEncoder[K]].encode(keys), options)
    )

  def dropIndex[K: BsonDocEncoder](keys: K): Unit < Async =
    ReactiveUtil.drain(base.dropIndex(summon[BsonDocEncoder[K]].encode(keys)))

  def dropIndex(indexName: String): Unit < Async = {
    ReactiveUtil.drain(base.dropIndex(indexName))
  }

  def dropCollection(): Unit < Async =
    ReactiveUtil.drain(base.drop())

  def listIndexes(): List[BsonDocument] < Async =
    ReactiveUtil.collect(base.listIndexes()).map(_.map(_.toBsonDocument))

case class FinderOptions(
    skip: Option[Int],
    readPreference: Option[ReadPreference],
    bufferSize: Int = 256
)

object FinderOptions:
  val default: FinderOptions = FinderOptions(None, None)

case class Finder(
    filter: BsonDocument,
    projection: Option[BsonDocument],
    order: Option[BsonDocument],
    options: FinderOptions,
    col: DocCollection
):

  private lazy val builder: FindPublisher[BsonDocument] =
    val base = options.readPreference match
      case Some(value) => col.base.withReadPreference(value)
      case None        => col.base
    val finder = base.find(filter)
    projection.foreach(finder.projection)
    order.foreach(finder.sort)
    options.skip.foreach(finder.skip)
    finder

  def one[T](using reader: BsonDocDecoder[T]): Maybe[T] < Async =
    ReactiveUtil
      .optional(builder.limit(1).first())
      .map(_.map(doc => reader.decode(doc).get))

  def list[T](limit: Option[Int] = None)(using
      reader: BsonDocDecoder[T]
  ): List[T] < Async =
    val pub = limit.map(builder.limit).getOrElse(builder)
    ReactiveUtil.collect(pub).map(_.map(reader.decode(_).get))

  def source[T: {BsonDocDecoder, Tag}](): Stream[T, Async] =
    Stream.init(
      ReactiveUtil
        .collect(builder)
        .map(_.map(summon[BsonDocDecoder[T]].decode(_).get))
    )
