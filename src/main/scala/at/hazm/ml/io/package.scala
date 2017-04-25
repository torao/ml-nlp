package at.hazm.ml

import scala.language.reflectiveCalls

package object io {
  def using[T <: {def close() :Unit}, U](resource:T)(f:(T) => U):U = try {
    f(resource)
  } finally {
    resource.close()
  }
  def using[T1 <: {def close() :Unit}, T2 <: {def close() :Unit}, U](r1:T1, r2:T2)(f:(T1,T2) => U):U = using(r1){ x1 =>
    using(r2){ x2 =>
      f(x1, x2)
    }
  }

}
