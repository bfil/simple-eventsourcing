package io.bfil.eventsourcing.util

import scala.collection.generic.CanBuildFrom
import scala.concurrent.{ExecutionContext, Future}

object FutureOps {
  def traverseSequentially[A, B, M[X] <: TraversableOnce[X]](in: M[A])
                                                            (fn: A => Future[B])
                                                            (implicit cbf: CanBuildFrom[M[A], B, M[B]], ec: ExecutionContext): Future[M[B]] =
    in.foldLeft(Future.successful(cbf(in))) { (fr, a) =>
      for (r <- fr; b <- fn(a)) yield (r += b)
    }.map(_.result())
}
