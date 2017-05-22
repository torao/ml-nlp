package at.hazm.ml.nlp

import at.hazm.ml.nlp.Paragraph.Sentence
import play.api.libs.json.{JsArray, JsObject, Json}

case class Paragraph(id:Int, sentences:Seq[Sentence]) {
  def toJSON:JsObject = Json.obj(
    "id" -> id,
    "sentences" -> Json.arr(sentences.map(_.toJSON))
  )
}

object Paragraph {

  def fromJSON(json:JsObject):Paragraph = Paragraph(
    id = (json \ "id").as[Int],
    sentences = (json \ "sentences").as[JsArray].value.map{ sentence =>
      Sentence(
        id = (sentence \ "id").as[Int],
        clauses = (sentence \ "clauses").as[JsArray].value.map{ clause =>
          Clause(
            id = (clause \ "id").as[Int],
            link = (clause \ "link").as[Int],
            rel = (clause \ "rel").as[String],
            score = (clause \ "score").as[Double],
            head = (clause \ "head").as[Int],
            func = (clause \ "func").as[Int],
            morphs = (clause \ "morphs").as[JsArray].value.map{ morph =>
              MorphIndex(
                id = (morph \ "id").as[Int],
                morphId = (morph \ "morph").as[Int],
                ne = (morph \ "ne").as[String]
              )
            }
          )
        }
      )
    }
  )

  case class MorphIndex(id:Int, morphId:Int, ne:String) {
    def toJSON:JsObject = Json.obj("id" -> id, "morph" -> morphId, "ne" -> ne)
  }

  // link="-1" rel="D" score="0.000000" head="12" func="13"
  case class Clause(id:Int, link:Int, rel:String, score:Double, head:Int, func:Int, morphs:Seq[MorphIndex]) {
    def toJSON:JsObject = Json.obj(
      "id" -> id,
      "link" -> link,
      "rel" -> rel,
      "score" -> score,
      "head" -> head,
      "func" -> func,
      "morphs" -> Json.arr(morphs.map(_.toJSON))
    )
  }

  case class Sentence(id:Int, clauses:Seq[Clause]) {
    def toJSON:JsObject = Json.obj(
      "id" -> id,
      "clauses" -> Json.arr(clauses.map(_.toJSON))
    )
  }

}