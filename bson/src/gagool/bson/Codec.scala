package gagool.bson

import org.bson.{BsonDocument, BsonValue}

import scala.util.{Success, Try}

/** Type-safe encoder that converts values of type A to representation C.
  * Contravariant in A (input type) and covariant in C (output type).
  */
trait Encoder[-A, +C]:
  self =>
  def encode(in: A): C
  def contramap[B](f: B => A): Encoder[B, C] = (in: B) => self.encode(f(in))

/** Type-safe decoder that converts representation C to values of type A.
  * Covariant in A (output type) and contravariant in C (input type).
  */
trait Decoder[+A, -C]:
  self =>
  def decode(in: C): Try[A]
  def map[B](f: A => B): Decoder[B, C] = (in: C) => self.decode(in).map(f)
  def flatMap[B](f: A => Try[B]): Decoder[B, C] = (in: C) =>
    self.decode(in).flatMap(f)
  def orElse[B >: A](that: => Try[B]): Decoder[B, C] = (in: C) =>
    self.decode(in).orElse(that)

/** Bidirectional codec combining encoder and decoder for type A and
  * representation C.
  */
trait Codec[A, C] extends Encoder[A, C] with Decoder[A, C] {
  self =>
  def imap[B](from: A => B, to: B => A): Codec[B, C] =
    new SimpleCodec[B, C](
      enc = in => self.encode(to(in)),
      dec = in => self.decode(in).map(from)
    )
}

type BsonValueEncoder[A] = Encoder[A, BsonValue]
type BsonValueDecoder[A] = Decoder[A, BsonValue]
type BsonValueCodec[A] = Codec[A, BsonValue]
type BsonDocEncoder[A] = Encoder[A, BsonDocument]
type BsonDocDecoder[A] = Decoder[A, BsonDocument]
type BsonDocCodec[A] = Codec[A, BsonDocument]

/** Factory methods and utilities for creating codecs.
  */
object Codec:
  def apply[A, C](encode: A => C, decode: C => Try[A]): Codec[A, C] =
    new SimpleCodec[A, C](
      enc = in => encode(in),
      dec = in => decode(in)
    )

  /** Sequence a list of Try values into a Try of list, failing fast on first
    * error.
    */
  def sequenceT[T](xs: List[Try[T]]): Try[List[T]] =
    xs.foldRight(Success(List.empty[T]): Try[List[T]]) { (h, acc) =>
      for { hh <- h; tt <- acc } yield hh :: tt
    }

  /** Create Codec from already existing ones */
  /** Create Codec from already existing ones using isomorphic mapping.
    */
  def imap[A, O](fromOrigin: O => A)(toOrigin: A => O)(using
      c: BsonValueCodec[O]
  ): BsonValueCodec[A] = c.imap(fromOrigin, toOrigin)

  /** Creates codec from an enum using it's 'values' function
    */
  def fromEnum[T](values: Array[T]): BsonValueCodec[T] = {
    val directMap = values.map(t => t.toString -> t).toMap
    val reverseMap = values.map(t => t -> t.toString).toMap
    import BaseCodecs.stringCodec
    imap(directMap.apply)(reverseMap.apply)
  }

  /** Creates codec from an enum-like structure that has a one to one mapping to
    * string.
    */
  def fromEnum[T](mapping: Seq[(String, T)]): BsonValueCodec[T] =
    val directMap = mapping.toMap
    val reverseMap = mapping.map(_.swap).toMap
    import BaseCodecs.stringCodec
    imap(directMap.apply)(reverseMap.apply)

/** Simple implementation of Codec using function literals.
  */
class SimpleCodec[A, C](enc: A => C, dec: C => Try[A]) extends Codec[A, C]:
  override def decode(in: C): Try[A] = dec(in)
  override def encode(in: A): C = enc(in)
