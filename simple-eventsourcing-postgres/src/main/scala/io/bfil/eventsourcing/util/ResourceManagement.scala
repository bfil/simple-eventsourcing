package io.bfil.eventsourcing.util

import scala.util.{Failure, Try}
import scala.util.control.NonFatal

trait ResourceManagement {
  def withResource[C <: AutoCloseable, R](resource: => C)(f: C => R): R =
    Try(resource).flatMap(resourceInstance =>
      try {
        val returnValue = f(resourceInstance)
        Try(resourceInstance.close()).map(_ => returnValue)
      } catch {
        case NonFatal(exceptionInFunction) =>
          try {
            resourceInstance.close()
            Failure(exceptionInFunction)
          } catch {
            case NonFatal(exceptionInClose) =>
              exceptionInFunction.addSuppressed(exceptionInClose)
              Failure(exceptionInFunction)
          }
      }
    ).get
}

object ResourceManagement extends ResourceManagement