package higherkindness.droste.contrib

import higherkindness.droste.util.newtypes.@@

object NewTypesSyntax {
  implicit class NewTypesOps[A](a: A) {
    def @@[B]: A @@ B = higherkindness.droste.util.newtypes.@@(a)
  }
}
