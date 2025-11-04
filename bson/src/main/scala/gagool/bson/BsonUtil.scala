package gagool.bson

import org.bson.BsonDocument
import BaseCodecs.given
import org.bson.BsonElement
import scala.jdk.CollectionConverters.*

/** Utilities for building BSON documents and MongoDB queries.
  */
object BsonUtil:

  val empty: BsonDocument = new BsonDocument()

  def doc(elements: BsonElement*): BsonDocument =
    new BsonDocument(elements.asJava)

  /** Efficiently makes a bson document
    */
  /** Efficiently creates a BSON document from key-value pairs.
    */
  def document[T: BsonValueCodec](pairs: (String, T)*): BsonDocument =
    val doc = new BsonDocument(pairs.size)
    var i = 0
    while i < pairs.size do
      val (k, v) = pairs(i)
      doc.put(k, summon[BsonValueCodec[T]].encode(v))
      i += 1
    doc

  /** Generate projection doc from case class fields */
  /** Generate projection document from case class fields.
    */
  def genProjection[T <: Product](cls: Class[T]): Option[BsonDocument] =
    val fields = cls.getDeclaredFields
      .filterNot(_.isSynthetic)
      .map(_.getName -> 1)
    if fields.isEmpty then None
    else Some(document(fields*))

  /** Create a MongoDB $set update document.
    */
  def set[T: BsonValueCodec](name: String, value: T): BsonDocument =
    doc("$set" -> doc(name -> value))

  given tupleToElement: Conversion[(String, BsonDocument), BsonElement] =
    case (k, v) => new BsonElement(k, v)

  given tupleToElement[T: BsonValueCodec]
      : Conversion[(String, T), BsonElement] =
    case (k, v) => new BsonElement(k, summon[BsonValueCodec[T]].encode(v))
    
  def idSel[T: BsonValueCodec](t: T): BsonDocument = doc("_id" -> t)

  extension (name: String)
    infix def wrapDoc(inner: BsonDocument): BsonDocument =
      new BsonDocument(name, inner)

    infix def wrap[T: BsonValueCodec](inner: T): BsonDocument =
      doc(name -> inner)

    infix def between(gte: Int, lt: Int): BsonDocument =
      doc("$gte" -> gte, "$lt" -> lt)
    infix def between(gte: Long, lt: Long): BsonDocument =
      doc("$gte" -> gte, "$lt" -> lt)
    infix def between(gte: Float, lt: Float): BsonDocument =
      doc("$gte" -> gte, "$lt" -> lt)
    infix def between(gte: Double, lt: Double): BsonDocument =
      doc("$gte" -> gte, "$lt" -> lt)

    infix def op[T: BsonValueCodec](operation: String, value: T): BsonDocument =
      doc(name -> doc(operation -> value))

    infix def lt(value: Int): BsonDocument = op("$lt", value)
    infix def lt(value: Long): BsonDocument = op("$lt", value)
    infix def lt(value: Float): BsonDocument = op("$lt", value)
    infix def lt(value: Double): BsonDocument = op("$lt", value)

    infix def lte(value: Int): BsonDocument = op("$lte", value)
    infix def lte(value: Long): BsonDocument = op("$lte", value)
    infix def lte(value: Float): BsonDocument = op("$lte", value)
    infix def lte(value: Double): BsonDocument = op("$lte", value)

    infix def gt(value: Int): BsonDocument = op("$gt", value)
    infix def gt(value: Long): BsonDocument = op("$gt", value)
    infix def gt(value: Float): BsonDocument = op("$gt", value)
    infix def gt(value: Double): BsonDocument = op("$gt", value)

    infix def gte(value: Int): BsonDocument = op("$gte", value)
    infix def gte(value: Long): BsonDocument = op("$gte", value)
    infix def gte(value: Float): BsonDocument = op("$gte", value)
    infix def gte(value: Double): BsonDocument = op("$gte", value)

    infix def is[T: BsonValueCodec](t: T): BsonDocument = doc(name -> t)
    infix def isNot[T: BsonValueCodec](t: T): BsonDocument = op("$ne", t)

    infix def in[T: BsonValueCodec](ts: Seq[T]): BsonDocument = op("$in", ts.toList)
    infix def in[T: BsonValueCodec](ts: Set[T]): BsonDocument = op("$in", ts)
    infix def in[T: BsonValueCodec](ts: List[T]): BsonDocument = op("$in", ts)

    infix def nin[T: BsonValueCodec](ts: Seq[T]): BsonDocument = op("$nin", ts.toList)
    infix def nin[T: BsonValueCodec](ts: Set[T]): BsonDocument = op("$nin", ts)
    infix def nin[T: BsonValueCodec](ts: List[T]): BsonDocument = op("$nin", ts)

    infix def include: BsonDocument = is(true)

    infix def isNotPresent: BsonDocument = op("$exists", false)
    infix def isPresent: BsonDocument = op("$exists", true)

  /** Validation utilities for common MongoDB patterns.
    */
  object Validators:

    /** Validate ObjectId format (24-character hex string). */
    def isValidObjectId(id: String): Boolean =
      id.length == 24 && id.forall("0123456789abcdefABCDEF".contains)

    /** Validate field name (no dots, no dollar sign prefix). */
    def isValidFieldName(name: String): Boolean =
      name.nonEmpty && !name.startsWith("$") && !name.contains(".")

    /** Validate collection name (no spaces, slashes, or special chars). */
    def isValidCollectionName(name: String): Boolean =
      name.nonEmpty &&
      !name.contains(" ") &&
      !name.contains("/") &&
      !name.contains("\\") &&
      !name.startsWith("system.")
