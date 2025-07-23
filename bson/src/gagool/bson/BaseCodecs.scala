package gagool.bson

import org.bson.{
  BsonArray,
  BsonBoolean,
  BsonDocument,
  BsonDouble,
  BsonInt32,
  BsonInt64,
  BsonNull,
  BsonString,
  BsonValue
}

import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/** Built-in codecs for primitive types and collections.
  */
object BaseCodecs:
  given bsonDocCodec: BsonDocCodec[BsonDocument] with
    def encode(s: BsonDocument): BsonDocument = s
    def decode(in: BsonDocument): Try[BsonDocument] = Success(in)


  /** Convert a doc-based codec into a value-based codec.
    * - encode: delegate to the doc codec (BsonDocument <: BsonValue)
    * - decode: accept any BsonValue; if it's a BsonDocument delegate, otherwise fail.
    */
  given docToValue[A](using doc: BsonDocCodec[A]): BsonValueCodec[A] =
    new BsonValueCodec[A]:
      override def encode(in: A): BsonValue =
        // doc.encode returns a BsonDocument, which is a BsonValue
        doc.encode(in)

      override def decode(in: BsonValue): Try[A] = in match
        case d: BsonDocument => doc.decode(d)
        case other =>
          Failure(
            new IllegalArgumentException(
              s"Expected BsonDocument to decode ${in.getClass.getSimpleName} -> ${other.getBsonType}"
            )
          )

  given bsonValueCodec: BsonValueCodec[BsonValue] with
    def encode(s: BsonValue): BsonValue = s

    def decode(in: BsonValue): Try[BsonValue] = Success(in)

  /** Codec for String values, encoding to BsonString.
    */
  given stringCodec: BsonValueCodec[String] with
    def encode(s: String): BsonValue = new BsonString(s)

    def decode(in: BsonValue): Try[String] = in match
      case s: BsonString => Success(s.getValue)
      case _: BsonNull   => Failure(new NoSuchElementException("BsonNull"))
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected BsonString but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for Int values with flexible numeric decoding.
    */
  given intCodec: BsonValueCodec[Int] with
    def encode(i: Int): BsonValue = new BsonInt32(i)

    def decode(in: BsonValue): Try[Int] = in match
      case i: BsonInt32  => Success(i.getValue)
      case l: BsonInt64  => Try(l.getValue.toInt) // may truncate
      case d: BsonDouble => Try(d.getValue.toInt)
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected numeric but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for Float values with flexible numeric decoding.
    */
  given floatCodec: BsonValueCodec[Float] with
    def encode(i: Float): BsonValue = new BsonDouble(i)

    def decode(in: BsonValue): Try[Float] = in match
      case i: BsonInt32  => Success(i.getValue.toFloat)
      case l: BsonInt64  => Try(l.getValue.toFloat) // may truncate
      case d: BsonDouble => Try(d.getValue.toFloat) // may truncate
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected numeric but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for Double values with flexible numeric decoding.
    */
  given doubleCodec: BsonValueCodec[Double] with
    def encode(i: Double): BsonValue = new BsonDouble(i)

    def decode(in: BsonValue): Try[Double] = in match
      case i: BsonInt32  => Success(i.getValue.toDouble)
      case l: BsonInt64  => Try(l.getValue.toDouble)
      case d: BsonDouble => Try(d.getValue)
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected numeric but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for Long values with flexible numeric decoding.
    */
  given longCodec: BsonValueCodec[Long] with
    def encode(l: Long): BsonValue = new BsonInt64(l)

    def decode(in: BsonValue): Try[Long] = in match
      case l: BsonInt64  => Success(l.getValue)
      case i: BsonInt32  => Success(i.getValue.toLong)
      case d: BsonDouble => Success(d.getValue.toLong)
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected numeric but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for Boolean values.
    */
  given boolCodec: BsonValueCodec[Boolean] with
    def encode(b: Boolean): BsonValue = new BsonBoolean(b)

    def decode(in: BsonValue): Try[Boolean] = in match
      case b: BsonBoolean => Success(b.getValue)
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected BsonBoolean but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for Set[T] encoded as BsonArray.
    */
  given setCodec[T](using elem: BsonValueCodec[T]): BsonValueCodec[Set[T]] =
    new BsonValueCodec[Set[T]]:
      override def encode(in: Set[T]): BsonValue =
        val array = new BsonArray()
        in.foreach(e => array.add(elem.encode(e)))
        array

      override def decode(in: BsonValue): Try[Set[T]] = in match
        case arr: BsonArray =>
          val tries = arr.getValues.asScala.toList.map(elem.decode)
          Codec.sequenceT(tries).map(_.toSet)
        case other =>
          Failure(
            new IllegalArgumentException(s"Expected BsonArray, got $other")
          )

  /** Codec for Option[A] where None encodes to BsonNull.
    */
  given optionCodec[A](using c: BsonValueCodec[A]): BsonValueCodec[Option[A]]
  with
    def encode(opt: Option[A]): BsonValue = opt match
      case Some(v) => c.encode(v)
      case None    => BsonNull.VALUE

    def decode(in: BsonValue): Try[Option[A]] =
      in match
        case _: BsonNull => Success(None)
        case any         => c.decode(any).map(Some.apply)

  /** Codec for List[A] encoded as BsonArray.
    */
  given listCodec[A](using c: BsonValueCodec[A]): BsonValueCodec[List[A]] with
    def encode(list: List[A]): BsonValue =
      new BsonArray(list.map(c.encode).asJava)

    def decode(in: BsonValue): Try[List[A]] = in match
      case arr: BsonArray =>
        val tries = arr.getValues.asScala.toList.map(v => c.decode(v))
        Codec.sequenceT(tries)
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected BsonArray but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for Map[String, T] encoded as BsonDocument.
    */
  given mapCodec[T](using
      elem: BsonValueCodec[T]
  ): BsonValueCodec[Map[String, T]] =
    new BsonValueCodec[Map[String, T]]:
      override def encode(in: Map[String, T]): BsonValue =
        val doc = new BsonDocument()
        in.foreach { case (k, v) =>
          doc.put(k, elem.encode(v))
        }
        doc

      override def decode(in: BsonValue): Try[Map[String, T]] = in match
        case doc: BsonDocument =>
          Try {
            val entries = doc.entrySet().asScala.toList
            val tries = entries.map { entry =>
              elem.decode(entry.getValue).map(value => entry.getKey -> value)
            }
            Codec.sequenceT(tries).map(_.toMap)
          }.flatten
        case other =>
          Failure(
            new IllegalArgumentException(s"Expected BsonDocument, got $other")
          )

  /** Codec for BigDecimal values stored as BsonString to preserve precision.
    */
  given bigDecimalCodec: BsonValueCodec[BigDecimal] with
    def encode(bd: BigDecimal): BsonValue = new BsonString(bd.toString)

    def decode(in: BsonValue): Try[BigDecimal] = in match
      case s: BsonString => Try(BigDecimal(s.getValue))
      case d: BsonDouble => Try(BigDecimal(d.getValue))
      case i: BsonInt32  => Try(BigDecimal(i.getValue))
      case l: BsonInt64  => Try(BigDecimal(l.getValue))
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected numeric or string but got ${other.getClass.getSimpleName}"
          )
        )

  /** Codec for BigInt values stored as BsonString to preserve precision.
    */
  given bigIntCodec: BsonValueCodec[BigInt] with
    def encode(bi: BigInt): BsonValue = new BsonString(bi.toString)

    def decode(in: BsonValue): Try[BigInt] = in match
      case s: BsonString => Try(BigInt(s.getValue))
      case i: BsonInt32  => Try(BigInt(i.getValue))
      case l: BsonInt64  => Try(BigInt(l.getValue))
      case d: BsonDouble => Try(BigInt(d.getValue.toLong))
      case other =>
        Failure(
          new IllegalArgumentException(
            s"Expected numeric or string but got ${other.getClass.getSimpleName}"
          )
        )

end BaseCodecs
