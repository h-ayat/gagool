package gagool.core

import kyo.*
import AllowUnsafe.embrace.danger
import scala.util.{Failure, Success, Try}

object TestUtil:

  extension [A](value: A) def upcast[B >: A]: B = value

  given CanEqual[org.bson.BsonValue, org.bson.BsonValue] = CanEqual.derived
  given CanEqual[org.bson.BsonString, org.bson.BsonString] = CanEqual.derived

  given [T] => CanEqual[Try[T], Try[T]] = CanEqual.derived
  given [T] => CanEqual[Success[T], Success[T]] = CanEqual.derived
  given [T] => CanEqual[Failure[T], Failure[T]] = CanEqual.derived
  given [T] => CanEqual[Try[T], Success[T]] = CanEqual.derived
  given [T] => CanEqual[Try[T], Failure[T]] = CanEqual.derived
  given [T] => CanEqual[Success[T], Try[T]] = CanEqual.derived
  given [T] => CanEqual[Failure[T], Try[T]] = CanEqual.derived
  given [T] => CanEqual[Option[T], Some[T]] = CanEqual.derived
  given [T] => CanEqual[Some[T], Option[T]] = CanEqual.derived

  extension [T](v: T < Async)
    def runSync(): T =
      KyoApp.Unsafe.runAndBlock(Duration.Infinity)(v).getOrThrow
