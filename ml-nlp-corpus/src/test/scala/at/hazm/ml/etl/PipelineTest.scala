package at.hazm.ml.etl

import java.io.{File, FileWriter}

import org.specs2.Specification
import org.specs2.execute.Result

class PipelineTest extends Specification {
  override def is =
    s2"""
文字列を読み込んで行で分割する: $e0
ソースを結合して読み込みが可能: $e1
リストソース: $e2
ファイルソース: $e3
URL ソース: $e4
"""

  def e0:Result = {
    val sample = "ABC\nDEFG\rHI\r\nJKLMN\r\n"
    val expected = List("ABC", "DEFG", "HI", "JKLMN")
    test(new StringSource(sample) :> new TextLine() :> { line:String => line }, expected)
  }

  def e1:Result = {
    val sample1 = "ABC\nDEFG\r\n"
    val sample2 = "HI\r\nJKLMN\r\n"
    val expected = List("ABC", "DEFG", "HI", "JKLMN")
    test(Source.sequence(new StringSource(sample1), new StringSource(sample2)) :> new TextLine(), expected)
  }

  def e2:Result = {
    val expected = List("ABC", "DEFG", "HI", "JKLMN")
    test(new ListSource(expected), expected)
  }

  def e3:Result = {
    val sample = "ABC\nDEFG\rHI\r\nJKLMN\r\n"
    val expected = List("ABC", "DEFG", "HI", "JKLMN")
    val file = File.createTempFile("pipeline-test", ".tmp")
    file.deleteOnExit()
    at.hazm.core.io.writeText(file)(_.write(sample))
    val result = test(new FileSource(file) :> new TextLine(), expected)
    result and (file.delete() must beTrue)
  }

  def e4:Result = {
    val sample = "ABC\nDEFG\rHI\r\nJKLMN\r\n"
    val expected = List("ABC", "DEFG", "HI", "JKLMN")
    val file = File.createTempFile("pipeline-test", ".tmp")
    file.deleteOnExit()
    at.hazm.core.io.writeText(file)(_.write(sample))
    val result = test(new URLSource(file.toURI.toURL) :> new TextLine(), expected)
    result and (file.delete() must beTrue)
  }

  private[this] def test(src:Source[String], expected:List[String]):Result = {
    val actual1 = src.toList
    val result1 = (expected.length === actual1.length) and expected.zip(actual1).map(x => x._1 === x._2).reduceLeft(_ and _)

    src.reset()

    val actual2 = src.toList
    val result2 = (expected.length === actual2.length) and expected.zip(actual2).map(x => x._1 === x._2).reduceLeft(_ and _)

    src.close()

    result1 and result2
  }

}
