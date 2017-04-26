package at.hazm.ml.nlp.knowledge

import java.io.File

import at.hazm.ml.io.Database

class Knowledge(file: File) {
  private[this] val db = new Database(file)

  val cyclopedia = new Cyclopedia(db)
  val source = new Source.DB(db)
}
