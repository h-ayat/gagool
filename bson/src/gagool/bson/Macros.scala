package gagool.bson

import org.bson.*

import scala.compiletime.{constValue, erasedValue, error, summonInline}
import scala.deriving.Mirror
import scala.util.{Failure, Success, Try}

import scala.reflect.ClassTag

object Macros:

  /* -------------------------------------------
   * helpers
   * ----------------------------------------- */

  private inline def summonAll[T <: Tuple]: List[Any] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (h *: t)   => summonInline[BsonValueCodec[h]] :: summonAll[t]

  private inline def fieldLabels[T <: Tuple]: List[String] =
    inline erasedValue[T] match
      case _: EmptyTuple => Nil
      case _: (h *: t)   => constValue[h].asInstanceOf[String] :: fieldLabels[t]

  private inline def summonClassTags[T <: Tuple]: List[ClassTag[?]] =
    inline erasedValue[T] match
      case _: (t *: ts)  => summonInline[ClassTag[t]] :: summonClassTags[ts]
      case _: EmptyTuple => Nil

  /** “Pretty” tag:
    *   - For top-level classes: simpleName
    *   - For members (like `Currencies.UsDollar`): Owner.SimpleName
    *   - If getting owner reliably is hard in macros, we approximate: keep only
    *     capitalized segments of the canonical name and join with '.'
    */
  private def prettyTag(c: Class[?]): String =
    val full = c.getName // e.g. a.b.Currencies$UsDollar
    // split on [.$] then keep parts that start with uppercase
    full
      .split("[.$]")
      .toList
      .filter(_.nonEmpty)
      .filter(s => s.headOption.exists(_.isUpper))
      .mkString(".")

  private def prettyTagFromLabel(full: String): String =
    full
      .split("[.$]")
      .toList
      .filter(_.nonEmpty)
      .filter(s => s.headOption.exists(_.isUpper))
      .mkString(".")

  private inline def mkDoc(kvs: (String, BsonValue)*): BsonDocument =
    val d = new BsonDocument(kvs.size)
    var i = 0
    while i < kvs.size do
      val (k, v) = kvs(i)
      d.put(k, v)
      i += 1
    d

  /* -------------------------------------------
   *  Product (case class) derivation (manual)
   *    Requires given codecs for *all* fields.
   * ----------------------------------------- */

  inline def product[T](using m: Mirror.ProductOf[T]): BsonDocCodec[T] =
    type Labels = m.MirroredElemLabels
    type Types = m.MirroredElemTypes

    val labels: List[String] = fieldLabels[Labels]
    val codecs: List[BsonValueCodec[?]] =
      summonAll[Types].asInstanceOf[List[BsonValueCodec[?]]]

    new SimpleCodec[T, BsonDocument](
      { t =>
        val p = t.asInstanceOf[Product]
        val d = new BsonDocument(labels.size)
        var i = 0
        while i < labels.size do
          val k = labels(i)
          val v = p.productElement(i)
          val c = codecs(i).asInstanceOf[BsonValueCodec[Any]]
          d.put(k, c.encode(v))
          i += 1
        d
      },
      { doc =>
        // decode all fields in order; fail fast
        Try {
          var i = 0
          val arr = new Array[Any](labels.size)
          while i < labels.size do
            val k = labels(i)
            val raw = doc.get(k)
            if raw == null then
              throw new NoSuchElementException(s"Missing field: $k")
            val c = codecs(i).asInstanceOf[BsonValueCodec[Any]]
            c.decode(raw) match
              case Failure(e) =>
                throw new Exception(s"Failed to decode field '$k'", e)
              case Success(v) => arr(i) = v
            i += 1
          m.fromProduct(Tuple.fromArray(arr))
        }
      }
    )

  inline def singleton[T <: Singleton](using
      v: ValueOf[T],
      ev: T =:= v.value.type
  ): BsonDocCodec[T] =
    new SimpleCodec[T, BsonDocument](
      _ => BsonUtil.empty,
      _ => Success(v.value)
    )

  /* -------------------------------------------
   * 3) Sealed ADT derivation (sum type) with _TAG
   *    Works for sealed traits / sealed abstract classes.
   *    Requires given codecs for each concrete subtype.
   * ----------------------------------------- */
  inline def sum[T](using m: Mirror.SumOf[T]): BsonDocCodec[T] =
    type Alts = m.MirroredElemTypes
    type AltNames = m.MirroredElemLabels

    val altCodecs: List[BsonDocCodec[? <: T]] =
      summonAll[Alts].asInstanceOf[List[BsonDocCodec[? <: T]]]

    val altNames: List[String] = fieldLabels[AltNames]

    val altClasses: List[Class[?]] =
      summonClassTags[Alts].map(_.runtimeClass)

    val canonicalNames: List[String] =
      altClasses.map(prettyTag)

    val nameToCodec: Map[String, BsonDocCodec[? <: T]] =
      canonicalNames.zip(altCodecs).toMap

    new SimpleCodec[T, BsonDocument](
      { t =>
        val ord = m.ordinal(t)
        val codec = altCodecs(ord).asInstanceOf[BsonDocCodec[Any]]
        val encoded = codec.encode(t)
        val runtimeTag = prettyTag(t.getClass)
        encoded.put("_TAG", new BsonString(runtimeTag))
        encoded
      },
      { doc =>
        val tagVal = doc.getString("_TAG")
        if tagVal == null
        then
          Failure(
            new NoSuchElementException("Missing discriminator field _TAG")
          )
        else
          val tag = tagVal.getValue
          nameToCodec.get(tag) match
            case Some(codec) =>
              codec.decode(doc)
            case None =>
              Failure(new NoSuchElementException(s"Unknown _TAG: $tag"))

      }
    )
