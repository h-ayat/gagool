package gagool.core

import fs2.Stream
import fs2.interop.reactivestreams.*
import cats.effect.Async

import com.mongodb.ReadPreference
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.InsertManyResult
import com.mongodb.client.result.InsertOneResult
import com.mongodb.reactivestreams.client.FindPublisher
import com.mongodb.reactivestreams.client.MongoCollection
import gagool.bson.BsonDocDecoder
import gagool.bson.BsonDocEncoder
import org.bson.BsonDocument
import org.bson.conversions.Bson

import scala.jdk.CollectionConverters.*
import fs2.interop.reactivestreams.fromPublisher
import cats.ApplicativeThrow
import scala.util.Try

class DocCollection[E[_]: Async](
    val base: MongoCollection[BsonDocument],
    readPreference: ReadPreference
) {

  def find[F: BsonDocEncoder, P: BsonDocEncoder, O: BsonDocEncoder](
      filter: F,
      projection: Option[P] = None,
      order: Option[O] = None,
      options: FinderOptions = FinderOptions.default
  ): Finder[E] = {
    Finder(
      summon[BsonDocEncoder[F]].encode(filter),
      projection.map(summon[BsonDocEncoder[P]].encode),
      order.map(summon[BsonDocEncoder[O]].encode),
      options,
      this
    )
  }

  def insert[T: BsonDocEncoder](doc: T): E[InsertOneResult] =
    fromPublisher(
      base.insertOne(summon[BsonDocEncoder[T]].encode(doc)),
      1
    ).compile.lastOrError

  def insertAll[T: BsonDocEncoder](docs: List[T]): E[InsertManyResult] =
    val bsonDocs = docs.map(summon[BsonDocEncoder[T]].encode).asJava
    fromPublisher(base.insertMany(bsonDocs), 1).compile.lastOrError

  def updateOne[F: BsonDocEncoder, U: BsonDocEncoder](
      filter: F,
      update: U,
      upsert: Boolean = false
  ): E[com.mongodb.client.result.UpdateResult] =
    val opts = new UpdateOptions().upsert(upsert)
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    val u: Bson = summon[BsonDocEncoder[U]].encode(update)
    fromPublisher(base.updateOne(f, u, opts), 1).compile.lastOrError

  def updateMany[F: BsonDocEncoder, U: BsonDocEncoder](
      filter: F,
      update: U,
      upsert: Boolean = false
  ): E[com.mongodb.client.result.UpdateResult] =
    val opts = new UpdateOptions().upsert(upsert)
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    val u: Bson = summon[BsonDocEncoder[U]].encode(update)
    fromPublisher(base.updateMany(f, u, opts), 1).compile.lastOrError

  def deleteOne[F: BsonDocEncoder](
      filter: F
  ): E[com.mongodb.client.result.DeleteResult] =
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    fromPublisher(base.deleteOne(f), 1).compile.lastOrError

  def deleteMany[F: BsonDocEncoder](
      filter: F
  ): E[com.mongodb.client.result.DeleteResult] =
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    fromPublisher(base.deleteMany(f), 1).compile.lastOrError

  def createIndex[K: BsonDocEncoder](
      keys: K,
      options: IndexOptions = new IndexOptions()
  ): E[String] =
    val k: Bson = summon[BsonDocEncoder[K]].encode(keys)
    fromPublisher(base.createIndex(k, options), 1).compile.lastOrError

  def dropIndex[K: BsonDocEncoder](keys: K): E[Unit] =
    val k: Bson = summon[BsonDocEncoder[K]].encode(keys)
    fromPublisher(base.dropIndex(k), 1).compile.drain

  def dropIndex(indexName: String): E[Unit] =
    fromPublisher(base.dropIndex(indexName), 1).compile.drain

  def dropCollection(): E[Unit] =
    fromPublisher(base.drop(), 1).compile.drain

  def listIndexes(): E[List[BsonDocument]] =
    fromPublisher(base.listIndexes(), 256)
      .map(_.toBsonDocument)
      .compile
      .toList
}

case class FinderOptions(
    skip: Option[Int],
    readPreference: Option[ReadPreference],
    bufferSize: Int = 256
)

object FinderOptions {
  val default = FinderOptions(None, None)
}

case class Finder[E[_]: Async](
    filter: BsonDocument,
    projection: Option[BsonDocument],
    order: Option[BsonDocument],
    options: FinderOptions,
    col: DocCollection[E]
) {

  private lazy val builder: FindPublisher[BsonDocument] =
    val base = options.readPreference match
      case Some(value) => col.base.withReadPreference(value)
      case None        => col.base
    val finder = base.find(filter)
    projection.foreach(finder.projection)
    order.foreach(finder.sort)
    options.skip.foreach(finder.skip)
    finder

  def one[T](using
      reader: BsonDocDecoder[T]
  ): E[Option[T]] =
    StreamUtil
      .unwrap(
        fromPublisher(builder.limit(1).first(), 1)
          .map(reader.decode)
      )
      .compile
      .last

  def list[T](
      limit: Option[Int] = None
  )(using reader: BsonDocDecoder[T]) = {
    StreamUtil
      .unwrap(
        fromPublisher(
          limit.map(builder.limit).getOrElse(builder),
          options.bufferSize
        )
          .map(reader.decode)
      )
      .compile
      .toList
  }

  def source[T: BsonDocDecoder](): Stream[E, T] =
    StreamUtil.unwrap(
      fromPublisher(builder, options.bufferSize)
        .map(summon[BsonDocDecoder[T]].decode)
    )
}
