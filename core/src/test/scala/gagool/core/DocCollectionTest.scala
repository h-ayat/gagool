package gagool.core

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.mongodb.ReadPreference
import com.mongodb.client.model.IndexOptions
import org.bson.BsonDocument
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import gagool.bson.BsonUtil.*
import gagool.bson.BsonUtil.given
import gagool.bson.BaseCodecs.given
import TestUtil.given
import gagool.bson.{BsonDocCodec, BsonValueCodec}
import gagool.bson.Codec

import scala.util.Try
import gagool.bson.BsonUtil

private case class TestDoc(name: String, value: Int)

class DocCollectionTest
    extends AnyFlatSpec
    with Matchers
    with MongodbProvider
    with BeforeAndAfterEach {

  override val dbName = "docCollectionTest"

  private val connection = new Connection[IO](
    ConnectionConfig(s"mongodb://$host:$port", dbName)
  )
  private val collection = connection.collection("test_collection")

  given BsonDocCodec[TestDoc] = Codec(
    (in: TestDoc) => doc("name" -> in.name, "value" -> in.value),
    (doc: BsonDocument) =>
      Try {
        TestDoc(
          doc.getString("name").getValue,
          doc.getInt32("value").getValue
        )
      }
  )

  override def beforeEach(): Unit = {
    super.beforeEach()
    collection.deleteMany(BsonUtil.empty).unsafeRunSync()
  }

  "DocCollection" should "insert and find a single document" in {
    val testDoc = TestDoc("test", 1)
    collection.insert(testDoc).unsafeRunSync()

    val result = collection.find(BsonUtil.empty).one[TestDoc].unsafeRunSync()
    result shouldBe Some(testDoc)
  }

  it should "insert multiple documents" in {
    val docs = List(
      TestDoc("test1", 1),
      TestDoc("test2", 2)
    )
    collection.insertAll(docs).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    results should contain theSameElementsAs docs
  }

  it should "update a single document" in {
    val document = TestDoc("test", 1)
    collection.insert(document).unsafeRunSync()

    val update = set("value", 2)
    collection.updateOne("name".is("test"), update).unsafeRunSync()

    val result = collection.find(BsonUtil.empty).one[TestDoc].unsafeRunSync()
    result shouldBe Some(TestDoc("test", 2))
  }

  it should "update multiple documents" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2)
    )
    collection.insertAll(docs).unsafeRunSync()

    val update = set("value", 3)
    collection.updateMany("name".is("test"), update).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    results.forall(_.value == 3) shouldBe true
  }

  it should "delete a single document" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2)
    )
    collection.insertAll(docs).unsafeRunSync()

    collection.deleteOne(doc("name" -> "test")).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    results.size shouldBe 1
  }

  it should "delete multiple documents" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2),
      TestDoc("other", 3)
    )
    collection.insertAll(docs).unsafeRunSync()

    collection.deleteMany(doc("name" -> "test")).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    results.size shouldBe 1
    results.head.name shouldBe "other"
  }

  it should "find documents with projection" in {
    val document = TestDoc("test", 1)
    collection.insert(document).unsafeRunSync()

    val projection = doc("name" -> 1)
    val result = collection
      .find(BsonUtil.empty, Some(projection))
      .one[BsonDocument]
      .unsafeRunSync()

    result.isDefined shouldBe true
    result.get.containsKey("name") shouldBe true
    result.get.containsKey("value") shouldBe false
  }

  it should "find documents with sorting" in {
    val docs = List(
      TestDoc("test1", 2),
      TestDoc("test2", 1)
    )
    collection.insertAll(docs).unsafeRunSync()

    val order = doc("value" -> 1)
    val results = collection
      .find(BsonUtil.empty, order = Some(order))
      .list[TestDoc]()
      .unsafeRunSync()

    results.size shouldBe 2
    results.head.value shouldBe 1
  }

  it should "find documents with skip" in {
    val docs = List(
      TestDoc("test1", 1),
      TestDoc("test2", 2)
    )
    collection.insertAll(docs).unsafeRunSync()

    val opts = FinderOptions(skip = Some(1), readPreference = None)
    val results = collection
      .find(BsonUtil.empty, options = opts)
      .list[TestDoc]()
      .unsafeRunSync()

    results.size shouldBe 1
  }

  it should "handle custom read preferences" in {
    val testDoc = TestDoc("test", 1)
    collection.insert(testDoc).unsafeRunSync()

    val opts = FinderOptions(
      skip = None,
      readPreference = Some(ReadPreference.primary())
    )
    val result = collection
      .find(BsonUtil.empty, options = opts)
      .one[TestDoc]
      .unsafeRunSync()

    result shouldBe Some(testDoc)
  }

  it should "create an index" in {
    val indexKeys = doc("name" -> 1)
    val indexName = collection.createIndex(indexKeys).unsafeRunSync()

    indexName should not be empty

    val indexes = collection.listIndexes().unsafeRunSync()
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should contain(indexName)
  }

  it should "create an index with options" in {
    val indexKeys = doc("value" -> 1)
    val options = new IndexOptions().unique(true).name("unique_value_index")
    val indexName =
      collection.createIndex(indexKeys, options).unsafeRunSync()

    indexName shouldBe "unique_value_index"

    val indexes = collection.listIndexes().unsafeRunSync()
    val uniqueIndex =
      indexes.find(_.getString("name").getValue == "unique_value_index")
    uniqueIndex shouldBe defined
    uniqueIndex.get.getBoolean("unique").getValue shouldBe true
  }

  it should "drop an index by keys" in {
    val indexKeys = doc("name" -> 1)
    val indexName = collection.createIndex(indexKeys).unsafeRunSync()

    collection.dropIndex(indexKeys).unsafeRunSync()

    val indexes = collection.listIndexes().unsafeRunSync()
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should not contain indexName
  }

  it should "drop an index by name" in {
    val indexKeys = doc("value" -> -1)
    val options = new IndexOptions().name("test_desc_index")
    val indexName =
      collection.createIndex(indexKeys, options).unsafeRunSync()

    collection.dropIndex("test_desc_index").unsafeRunSync()

    val indexes = collection.listIndexes().unsafeRunSync()
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should not contain "test_desc_index"
  }

  it should "list all indexes" in {
    val indexes = collection.listIndexes().unsafeRunSync()

    indexes should not be empty
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should contain("_id_")
  }

  it should "create compound index" in {
    val compoundKeys = doc("name" -> 1, "value" -> -1)
    val indexName =
      collection.createIndex(compoundKeys).unsafeRunSync()

    indexName should not be empty

    val indexes = collection.listIndexes().unsafeRunSync()
    val compoundIndex =
      indexes.find(_.getString("name").getValue == indexName)
    compoundIndex shouldBe defined
  }
}
