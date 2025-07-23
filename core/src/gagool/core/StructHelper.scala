package gagool.core

import org.reactivestreams.{Publisher, Subscriber, Subscription}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.Console.{in, out}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object StructHelper:

  extension [T](pub: Publisher[T])(using ec: ExecutionContext)
    def toFuture: Future[T] = publisherToFutureOption(pub)

  val noneFuture: Future[Option[Nothing]] = Future.successful(None)
  private val noneTry: Try[Option[Nothing]] = Success(None)

  def absolveFutureTries[T](futureList: Future[List[Try[T]]])(using
      ec: ExecutionContext
  ): Future[List[T]] =
    futureList.transform:
      case Success(listOfTries) =>
        val (successes, failures) = listOfTries.partitionMap:
          case Success(value)     => Left(value)
          case Failure(exception) => Right(exception)

        if failures.isEmpty then Success(successes)
        else Failure(failures.head)

      case Failure(exception) =>
        Failure(exception)

  def absolveOptionalTry[T](
      in: Future[Option[Try[T]]]
  )(using ec: ExecutionContext): Future[Option[T]] =
    in.transform:
      case Failure(e)                    => Failure(e)
      case Success(None)                 => noneTry
      case Success(Some(Success(value))) => Success(Some(value))
      case Success(Some(Failure(e)))     => Failure(e)

  def publisherToFutureOption[T](
      pub: Publisher[T]
  )(implicit ec: ExecutionContext): Future[T] = {
    val promise = Promise[T]()
    pub.subscribe(new Subscriber[T] {
      override def onSubscribe(s: Subscription): Unit = s.request(1)
      override def onNext(t: T): Unit = promise.trySuccess(t)
      override def onError(t: Throwable): Unit = promise.tryFailure(t)
      override def onComplete(): Unit = ()
    })
    promise.future
  }

  def publisherToFutureList[T](
      publisher: Publisher[T]
  )(using ec: ExecutionContext): Future[List[T]] = {
    val promise = Promise[List[T]]()
    val elements =
      new java.util.ArrayList[T]()
    val subscriptionRef = AtomicReference[Option[Subscription]](None)
    val completed = AtomicBoolean(false)

    publisher.subscribe(new Subscriber[T] {
      override def onSubscribe(s: Subscription): Unit = {
        if (subscriptionRef.compareAndSet(None, Some(s))) {
          s.request(Long.MaxValue)
        } else {
          s.cancel()
        }
      }

      override def onNext(t: T): Unit = {
        if (!completed.get()) {
          elements.add(t)
        }
      }

      override def onError(t: Throwable): Unit = {
        if (completed.compareAndSet(false, true)) {
          subscriptionRef.get().foreach(_.cancel())
          promise.failure(t)
        }
      }

      override def onComplete(): Unit = {
        if (completed.compareAndSet(false, true)) {
          val scalaList = {
            val size = elements.size()
            val result = List.newBuilder[T]
            var i = 0
            while (i < size) {
              result += elements.get(i)
              i += 1
            }
            result.result()
          }
          promise.success(scalaList)
        }
      }
    })

    promise.future
  }
