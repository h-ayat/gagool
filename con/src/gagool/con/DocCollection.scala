package gagool.con

import com.mongodb.ReadPreference
import com.mongodb.client.model.IndexOptions
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.result.InsertManyResult
import com.mongodb.client.result.InsertOneResult
import com.mongodb.reactivestreams.client.FindPublisher
import com.mongodb.reactivestreams.client.MongoCollection
import gagool.bson.BsonDocDecoder
import gagool.bson.BsonDocEncoder
import gagool.core.StructHelper.absolveFutureTries
import gagool.core.StructHelper.toFuture
import org.bson.BsonDocument
import org.bson.conversions.Bson
import org.reactivestreams.Publisher

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import org.bson.Document

class DocCollection(
    val base: MongoCollection[BsonDocument],
    readPreference: ReadPreference
) {

  def find[F: BsonDocEncoder, P: BsonDocEncoder, O: BsonDocEncoder](
      filter: F,
      projection: Option[P] = None,
      order: Option[O] = None,
      skip: Option[Int] = None
  ): Finder = {
    Finder(
      summon[BsonDocEncoder[F]].encode(filter),
      projection.map(summon[BsonDocEncoder[P]].encode),
      order.map(summon[BsonDocEncoder[O]].encode),
      skip,
      readPreference,
      this
    )
  }

  def insert[T: BsonDocEncoder](
      doc: T
  )(using ExecutionContext): Future[InsertOneResult] =
    base.insertOne(summon[BsonDocEncoder[T]].encode(doc)).toFuture

  def insertAll[T: BsonDocEncoder](
      docs: List[T]
  )(using ExecutionContext): Future[InsertManyResult] =
    val bsonDocs = docs.map(summon[BsonDocEncoder[T]].encode).asJava
    base.insertMany(bsonDocs).toFuture

  def updateOne[F: BsonDocEncoder, U: BsonDocEncoder](
      filter: F,
      update: U,
      upsert: Boolean = false
  )(using ExecutionContext): Future[com.mongodb.client.result.UpdateResult] =
    val options = new UpdateOptions().upsert(upsert)
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    val u: Bson = summon[BsonDocEncoder[U]].encode(update)
    base.updateOne(f, u, options).toFuture

  def updateMany[F: BsonDocEncoder, U: BsonDocEncoder](
      filter: F,
      update: U,
      upsert: Boolean = false
  )(using ExecutionContext): Future[com.mongodb.client.result.UpdateResult] =
    val options = new UpdateOptions().upsert(upsert)
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    val u: Bson = summon[BsonDocEncoder[U]].encode(update)
    base.updateMany(f, u, options).toFuture

  def deleteOne[F: BsonDocEncoder](
      filter: F
  )(using ExecutionContext): Future[com.mongodb.client.result.DeleteResult] =
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    base.deleteOne(f).toFuture

  def deleteMany[F: BsonDocEncoder](
      filter: F
  )(using ExecutionContext): Future[com.mongodb.client.result.DeleteResult] =
    val f: Bson = summon[BsonDocEncoder[F]].encode(filter)
    base.deleteMany(f).toFuture

  def createIndex[K: BsonDocEncoder](
      keys: K,
      options: IndexOptions = new IndexOptions()
  )(using ExecutionContext): Future[String] =
    val k: Bson = summon[BsonDocEncoder[K]].encode(keys)
    base.createIndex(k, options).toFuture

  def dropIndex[K: BsonDocEncoder](
      keys: K
  )(using ExecutionContext): Future[Unit] =
    val k: Bson = summon[BsonDocEncoder[K]].encode(keys)
    base.dropIndex(k).toFuture.map(_ => ())

  def dropIndex(
      indexName: String
  )(using ExecutionContext): Future[Unit] =
    base.dropIndex(indexName).toFuture.map(_ => ())

  def listIndexes()(using ExecutionContext): Future[List[BsonDocument]] =
    gagool.core.StructHelper
      .publisherToFutureList(base.listIndexes())
      .map(_.map(_.toBsonDocument))
}

case class Finder(
    filter: BsonDocument,
    projection: Option[BsonDocument],
    order: Option[BsonDocument],
    skip: Option[Int],
    preference: ReadPreference,
    col: DocCollection
) {
  import gagool.core.StructHelper.{
    absolveOptionalTry,
    publisherToFutureList,
    publisherToFutureOption
  }

  private lazy val builder: FindPublisher[BsonDocument] =
    val finder = col.base.find(filter)
    projection.foreach(finder.projection)
    order.foreach(finder.sort)
    skip.foreach(finder.skip)
    finder

  def readPreference(preference: ReadPreference): Finder =
    this.copy(preference = preference)

  def one[T](using
      reader: BsonDocDecoder[T],
      ec: ExecutionContext
  ): Future[Option[T]] =
    absolveOptionalTry(
      publisherToFutureOption(
        builder.limit(1).first()
      )
        .map(Option.apply)
        .map(_.map(reader.decode))
    )

  def list[T](
      limit: Int = -1
  )(using reader: BsonDocDecoder[T], ec: ExecutionContext): Future[List[T]] =
    absolveFutureTries(
      publisherToFutureList(
        if limit > 0 then builder.limit(limit) else builder
      ).map(_.map(reader.decode))
    )

  def source[T: BsonDocDecoder](): Publisher[BsonDocument] = builder
}
