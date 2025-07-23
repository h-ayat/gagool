package gagool.con

import scala.util.{Failure, Success, Try}

object TestUtil:

  extension [A](value: A) def upcast[B >: A]: B = value

  given CanEqual[org.bson.BsonValue, org.bson.BsonValue] = CanEqual.derived
  given CanEqual[org.bson.BsonString, org.bson.BsonString] = CanEqual.derived

  given [T]: CanEqual[Try[T], Try[T]] = CanEqual.derived

  // Enable comparison between Success[T] and Success[T]
  given [T]: CanEqual[Success[T], Success[T]] = CanEqual.derived

  // Enable comparison between Failure[T] and Failure[T]
  given [T]: CanEqual[Failure[T], Failure[T]] = CanEqual.derived

  // Enable comparison between Try subtypes
  given [T]: CanEqual[Try[T], Success[T]] = CanEqual.derived
  given [T]: CanEqual[Try[T], Failure[T]] = CanEqual.derived
  given [T]: CanEqual[Success[T], Try[T]] = CanEqual.derived
  given [T]: CanEqual[Failure[T], Try[T]] = CanEqual.derived
  given [T]: CanEqual[Option[T], Some[T]] = CanEqual.derived
  given [T]: CanEqual[Some[T], Option[T]] = CanEqual.derived
