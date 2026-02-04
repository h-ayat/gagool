package gagool.core

import cats.ApplicativeThrow
import fs2.Stream
import cats.effect.Async
import scala.util.Try

object StreamUtil {
  def unwrap[F[_]: ApplicativeThrow, T](s: Stream[F, Try[T]]): Stream[F, T] =
    s.evalMap(ApplicativeThrow[F].fromTry)
}
