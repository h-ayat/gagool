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
    val _ = collection.deleteMany(BsonUtil.empty).unsafeRunSync()
  }

  "DocCollection" should "insert and find a single document" in {
    val testDoc = TestDoc("test", 1)
    val _ = collection.insert(testDoc).unsafeRunSync()

    val result = collection.find(BsonUtil.empty).one[TestDoc].unsafeRunSync()
    val _ = result shouldBe Some(testDoc)
  }

  it should "insert multiple documents" in {
    val docs = List(
      TestDoc("test1", 1),
      TestDoc("test2", 2)
    )
    val _ = collection.insertAll(docs).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    val _ = results should contain theSameElementsAs docs
  }

  it should "update a single document" in {
    val document = TestDoc("test", 1)
    val _ = collection.insert(document).unsafeRunSync()

    val update = set("value", 2)
    val _ = collection.updateOne("name".is("test"), update).unsafeRunSync()

    val result = collection.find(BsonUtil.empty).one[TestDoc].unsafeRunSync()
    val _ = result shouldBe Some(TestDoc("test", 2))
  }

  it should "update multiple documents" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2)
    )
    val _ = collection.insertAll(docs).unsafeRunSync()

    val update = set("value", 3)
    val _ = collection.updateMany("name".is("test"), update).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    results.forall(_.value == 3) shouldBe true
  }

  it should "delete a single document" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2)
    )
    val _ = collection.insertAll(docs).unsafeRunSync()

    val _ = collection.deleteOne(doc("name" -> "test")).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    val _ = results.size shouldBe 1
  }

  it should "delete multiple documents" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2),
      TestDoc("other", 3)
    )
    val _ = collection.insertAll(docs).unsafeRunSync()

    val _ = collection.deleteMany(doc("name" -> "test")).unsafeRunSync()

    val results =
      collection.find(BsonUtil.empty).list[TestDoc]().unsafeRunSync()
    val _ = results.size shouldBe 1
    val _ = results.head.name shouldBe "other"
  }

  it should "find documents with projection" in {
    val document = TestDoc("test", 1)
    val _ = collection.insert(document).unsafeRunSync()

    val projection = doc("name" -> 1)
    val result = collection
      .find(BsonUtil.empty, Some(projection))
      .one[BsonDocument]
      .unsafeRunSync()

    val _ = result.isDefined shouldBe true
    val _ = result.get.containsKey("name") shouldBe true
    val _ = result.get.containsKey("value") shouldBe false
  }

  it should "find documents with sorting" in {
    val docs = List(
      TestDoc("test1", 2),
      TestDoc("test2", 1)
    )
    val _ = collection.insertAll(docs).unsafeRunSync()

    val order = doc("value" -> 1)
    val results = collection
      .find(BsonUtil.empty, order = Some(order))
      .list[TestDoc]()
      .unsafeRunSync()

    val _ = results.size shouldBe 2
    val _ = results.head.value shouldBe 1
  }

  it should "find documents with skip" in {
    val docs = List(
      TestDoc("test1", 1),
      TestDoc("test2", 2)
    )
    val _ = collection.insertAll(docs).unsafeRunSync()

    val opts = FinderOptions(skip = Some(1), readPreference = None)
    val results = collection
      .find(BsonUtil.empty, options = opts)
      .list[TestDoc]()
      .unsafeRunSync()

    val _ = results.size shouldBe 1
  }

  it should "handle custom read preferences" in {
    val testDoc = TestDoc("test", 1)
    val _ = collection.insert(testDoc).unsafeRunSync()

    val opts = FinderOptions(
      skip = None,
      readPreference = Some(ReadPreference.primary())
    )
    val result = collection
      .find(BsonUtil.empty, options = opts)
      .one[TestDoc]
      .unsafeRunSync()

    val _ = result shouldBe Some(testDoc)
  }

  it should "create an index" in {
    val indexKeys = doc("name" -> 1)
    val indexName = collection.createIndex(indexKeys).unsafeRunSync()

    val _ = indexName should not be empty

    val indexes = collection.listIndexes().unsafeRunSync()
    val indexNames = indexes.map(_.getString("name").getValue)
    val _ = indexNames should contain(indexName)
  }

  it should "create an index with options" in {
    val indexKeys = doc("value" -> 1)
    val options = new IndexOptions().unique(true).name("unique_value_index")
    val indexName =
      collection.createIndex(indexKeys, options).unsafeRunSync()

    val _ = indexName shouldBe "unique_value_index"

    val indexes = collection.listIndexes().unsafeRunSync()
    val uniqueIndex =
      indexes.find(_.getString("name").getValue == "unique_value_index")
    val _ = uniqueIndex shouldBe defined
    val _ = uniqueIndex.get.getBoolean("unique").getValue shouldBe true
  }

  it should "drop an index by keys" in {
    val indexKeys = doc("name" -> 1)
    val indexName = collection.createIndex(indexKeys).unsafeRunSync()

    val _ = collection.dropIndex(indexKeys).unsafeRunSync()

    val indexes = collection.listIndexes().unsafeRunSync()
    val indexNames = indexes.map(_.getString("name").getValue)
    val _ = indexNames should not contain indexName
  }

  it should "drop an index by name" in {
    val indexKeys = doc("value" -> -1)
    val options = new IndexOptions().name("test_desc_index")
    val indexName =
      collection.createIndex(indexKeys, options).unsafeRunSync()

    collection.dropIndex("test_desc_index").unsafeRunSync()

    val indexes = collection.listIndexes().unsafeRunSync()
    val indexNames = indexes.map(_.getString("name").getValue)
    val _ = indexNames should not contain "test_desc_index"
  }

  it should "list all indexes" in {
    val indexes = collection.listIndexes().unsafeRunSync()

    val _ = indexes should not be empty
    val indexNames = indexes.map(_.getString("name").getValue)
    val _ = indexNames should contain("_id_")
  }

  it should "create compound index" in {
    val compoundKeys = doc("name" -> 1, "value" -> -1)
    val indexName =
      collection.createIndex(compoundKeys).unsafeRunSync()

    val _ = indexName should not be empty

    val indexes = collection.listIndexes().unsafeRunSync()
    val compoundIndex =
      indexes.find(_.getString("name").getValue == indexName)
    val _ = compoundIndex shouldBe defined
  }
}
