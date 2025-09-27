package gagool.con

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

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration.*
import scala.util.{Success, Try}
import gagool.bson.BsonUtil

private case class TestDoc(name: String, value: Int)

class DocCollectionTest
    extends AnyFlatSpec
    with Matchers
    with MongodbProvider
    with BeforeAndAfterEach {

  override val dbName = "docCollectionTest"
  given ExecutionContext = ExecutionContext.global

  private val connection = new Connection(
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
    // Clean the collection before each test
    Await.result(collection.deleteMany(BsonUtil.empty), 5.seconds)
  }

  "DocCollection" should "insert and find a single document" in {
    val doc = TestDoc("test", 1)
    Await.result(collection.insert(doc), 5.seconds)

    val result =
      Await.result(collection.find(BsonUtil.empty).one[TestDoc], 5.seconds)
    result shouldBe Some(doc)
  }

  it should "insert multiple documents" in {
    val docs = List(
      TestDoc("test1", 1),
      TestDoc("test2", 2)
    )
    Await.result(collection.insertAll(docs), 5.seconds)

    val results =
      Await.result(collection.find(BsonUtil.empty).list[TestDoc](), 5.seconds)
    results should contain theSameElementsAs docs
  }

  it should "update a single document" in {
    val document = TestDoc("test", 1)
    Await.result(collection.insert(document), 5.seconds)

    val update = set("value", 2)
    Await.result(collection.updateOne("name".is("test"), update), 5.seconds)

    val result =
      Await.result(collection.find(BsonUtil.empty).one[TestDoc], 5.seconds)
    result shouldBe Some(TestDoc("test", 2))
  }

  it should "update multiple documents" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2)
    )
    Await.result(collection.insertAll(docs), 5.seconds)

    val update = set("value", 3)
    Await.result(
      collection.updateMany("name".is("test"), update),
      5.seconds
    )

    val results =
      Await.result(collection.find(BsonUtil.empty).list[TestDoc](), 5.seconds)
    results.forall(_.value == 3) shouldBe true
  }

  it should "delete a single document" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2)
    )
    Await.result(collection.insertAll(docs), 5.seconds)

    Await.result(collection.deleteOne(doc("name" -> "test")), 5.seconds)

    val results =
      Await.result(collection.find(BsonUtil.empty).list[TestDoc](), 5.seconds)
    results.size shouldBe 1
  }

  it should "delete multiple documents" in {
    val docs = List(
      TestDoc("test", 1),
      TestDoc("test", 2),
      TestDoc("other", 3)
    )
    Await.result(collection.insertAll(docs), 5.seconds)

    Await.result(collection.deleteMany(doc("name" -> "test")), 5.seconds)

    val results =
      Await.result(collection.find(BsonUtil.empty).list[TestDoc](), 5.seconds)
    results.size shouldBe 1
    results.head.name shouldBe "other"
  }

  it should "find documents with projection" in {
    val document = TestDoc("test", 1)
    Await.result(collection.insert(document), 5.seconds)

    val projection = doc("name" -> 1)
    val result = Await.result(
      collection.find(BsonUtil.empty, Some(projection)).one[BsonDocument],
      5.seconds
    )

    result.isDefined shouldBe true
    result.get.containsKey("name") shouldBe true
    result.get.containsKey("value") shouldBe false
  }

  it should "find documents with sorting" in {
    val docs = List(
      TestDoc("test1", 2),
      TestDoc("test2", 1)
    )
    Await.result(collection.insertAll(docs), 5.seconds)

    val order = doc("value" -> 1)
    val results = Await.result(
      collection.find(BsonUtil.empty, order = Some(order)).list[TestDoc](),
      5.seconds
    )

    results.size shouldBe 2
    results.head.value shouldBe 1
  }

  it should "find documents with skip" in {
    val docs = List(
      TestDoc("test1", 1),
      TestDoc("test2", 2)
    )
    Await.result(collection.insertAll(docs), 5.seconds)

    val results = Await.result(
      collection.find(BsonUtil.empty, skip = Some(1)).list[TestDoc](),
      5.seconds
    )

    results.size shouldBe 1
    results.head.name shouldBe "test2"
  }

  it should "handle custom read preferences" in {
    val doc = TestDoc("test", 1)
    Await.result(collection.insert(doc), 5.seconds)

    val finder =
      collection.find(BsonUtil.empty).readPreference(ReadPreference.primary())
    val result = Await.result(finder.one[TestDoc], 5.seconds)

    result shouldBe Some(doc)
  }

  it should "create an index" in {
    val indexKeys = doc("name" -> 1)
    val indexName = Await.result(collection.createIndex(indexKeys), 10.seconds)

    indexName should not be empty

    val indexes = Await.result(collection.listIndexes(), 5.seconds)
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should contain(indexName)
  }

  it should "create an index with options" in {
    val indexKeys = doc("value" -> 1)
    val options = new IndexOptions().unique(true).name("unique_value_index")
    val indexName = Await.result(collection.createIndex(indexKeys, options), 10.seconds)

    indexName shouldBe "unique_value_index"

    val indexes = Await.result(collection.listIndexes(), 5.seconds)
    val uniqueIndex = indexes.find(_.getString("name").getValue == "unique_value_index")
    uniqueIndex shouldBe defined
    uniqueIndex.get.getBoolean("unique").getValue shouldBe true
  }

  it should "drop an index by keys" in {
    val indexKeys = doc("name" -> 1)
    val indexName = Await.result(collection.createIndex(indexKeys), 10.seconds)

    Await.result(collection.dropIndex(indexKeys), 5.seconds)

    val indexes = Await.result(collection.listIndexes(), 5.seconds)
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should not contain indexName
  }

  it should "drop an index by name" in {
    val indexKeys = doc("value" -> -1)
    val options = new IndexOptions().name("test_desc_index")
    val indexName = Await.result(collection.createIndex(indexKeys, options), 10.seconds)

    Await.result(collection.dropIndex("test_desc_index"), 5.seconds)

    val indexes = Await.result(collection.listIndexes(), 5.seconds)
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should not contain "test_desc_index"
  }

  it should "list all indexes" in {
    val indexes = Await.result(collection.listIndexes(), 5.seconds)

    indexes should not be empty
    val indexNames = indexes.map(_.getString("name").getValue)
    indexNames should contain("_id_")
  }

  it should "create compound index" in {
    val compoundKeys = doc("name" -> 1, "value" -> -1)
    val indexName = Await.result(collection.createIndex(compoundKeys), 10.seconds)

    indexName should not be empty

    val indexes = Await.result(collection.listIndexes(), 5.seconds)
    val compoundIndex = indexes.find(_.getString("name").getValue == indexName)
    compoundIndex shouldBe defined
  }
}
