package at.hazm.ml.nlp

import at.hazm.ml.nlp.Paragraph.Sentence
import play.api.libs.json.{JsArray, JsObject, Json}

import scala.annotation.tailrec
import scala.collection.mutable

case class Paragraph(id:Int, sentences:Seq[Sentence]) {

  /**
    * このパラグラフに含まれるすべての形態素インデックスを文内の出現順に並べたものを返します。
    *
    * @return 文の形態素インデックス
    */
  def toIndices:Seq[Int] = sentences.flatMap(_.clauses.flatMap(_.morphs.map(_.morphId)))

  def toJSON:JsObject = Json.obj(
    "id" -> id,
    "sentences" -> JsArray(sentences.map(_.toJSON))
  )
}

object Paragraph {

  def fromJSON(json:JsObject):Paragraph = Paragraph(
    id = (json \ "id").as[Int],
    sentences = (json \ "sentences").as[JsArray].value.map { sentence =>
      Sentence(
        id = (sentence \ "id").as[Int],
        clauses = (sentence \ "clauses").as[JsArray].value.map { clause =>
          Clause(
            id = (clause \ "id").as[Int],
            link = (clause \ "link").as[Int],
            rel = (clause \ "rel").as[String],
            score = (clause \ "score").as[Double],
            head = (clause \ "head").as[Int],
            func = (clause \ "func").as[Int],
            morphs = (clause \ "morphs").as[JsArray].value.map { morph =>
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

  /**
    * 文節クラス。
    *
    * @param id     文節 ID (文の中で一意)
    * @param link   係り先の文節 ID (係り受け先がない場合は負の値)
    * @param rel    係り受け関係
    * @param score  係り受けの強さ (大きい方がかかりやすい)
    * @param head   主辞の形態素 ID
    * @param func   機能語の形態素 ID
    * @param morphs この文節の形態素
    */
  case class Clause(id:Int, link:Int, rel:String, score:Double, head:Int, func:Int, morphs:Seq[MorphIndex]) {
    // link="-1" rel="D" score="0.000000" head="12" func="13"
    def toJSON:JsObject = Json.obj(
      "id" -> id,
      "link" -> link,
      "rel" -> rel,
      "score" -> score,
      "head" -> head,
      "func" -> func,
      "morphs" -> JsArray(morphs.map(_.toJSON))
    )
  }

  case class Sentence(id:Int, clauses:Seq[Clause]) {

    /**
      * 係り受けのグラフ構造を解析してこの文をより単純化した複数の文に変換します。
      *
      * @return 単純化された文
      */
    def breakdown():Seq[Sentence] = {

      @tailrec
      def _join(clause:Clause, map:Map[Int, Clause], buf:mutable.Buffer[Clause] = mutable.Buffer()):Seq[Clause] = {
        buf.append(clause)
        if(clause.link >= 0) _join(map(clause.link), map) else buf
      }

      val map = clauses.groupBy(_.id).mapValues(_.head)
      (map.keySet -- clauses.map(_.link).toSet).toSeq.map(id => map(id)).map { head =>
        Sentence(id, _join(head, map))
      }
    }

    /**
      * 指定されたコーパスに基づいてこの文を文字列に戻します。
      *
      * @param corpus コーパス
      * @return 文字列としての文
      */
    def makePlainText(corpus:Corpus):String = {
      val morphIds = clauses.flatMap(_.morphs.map(_.morphId))
      val morphs = corpus.vocabulary.getAll(morphIds)
      morphIds.map(id => morphs(id).surface).mkString
    }

    def toJSON:JsObject = Json.obj(
      "id" -> id,
      "clauses" -> JsArray(clauses.map(_.toJSON))
    )
  }

}