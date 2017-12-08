/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.tools

import java.io.{BufferedReader, InputStreamReader}

import at.hazm.ml.nlp.Corpus
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork
import org.slf4j.LoggerFactory

object InteractiveShell {
  private[this] val logger = LoggerFactory.getLogger(getClass.getName.dropRight(1))

  def start(corpus:Corpus, lstm:MultiLayerNetwork):Unit = {
    val in = new BufferedReader(new InputStreamReader(System.in))
    val buffer = new StringBuilder()

    def _loop():Unit = {
      System.out.print(">> ")
      val text = in.readLine()
      if(text != null && !text.equalsIgnoreCase("quit")) {
        if(text.trim().nonEmpty) {
          try {
            val doc = Paragraph2PerforatedSentence.transform(corpus, Wikipedia2Corpus.transform(corpus, -1, text))
            if(doc.sentences.nonEmpty) {
              doc.sentences.flatten.map { sentenceId =>
                corpus.perforatedSentences(sentenceId)
              }.foreach { sentence =>
                System.out.println(s">> ${sentence.mkString(corpus)}")
              }
              PerforatedSentence2LSTM.predict(corpus, doc, lstm, 10).map { perforatedId =>
                val sentence = corpus.perforatedSentences.apply(perforatedId)
                sentence.mkString(corpus)
              }.foreach(s => println(s"<< $s"))
              buffer.append(text)
            } else println("<< 認識できる文が含まれていませんでした。")
          } catch {
            case ex:Throwable =>
              ex.printStackTrace()
          }
        }
        _loop()
      }
    }

    logger.info("LSTM 文章予測インタラクティブ・シェルを開始しています (quit で終了)")
    _loop()
  }

}
