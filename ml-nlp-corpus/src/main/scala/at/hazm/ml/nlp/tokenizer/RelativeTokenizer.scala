/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.tokenizer

import at.hazm.ml.nlp.model.{RelativeDocument, Syntax, Token}

/**
  * 文の係り受けを解析し文節付きのトークンに分割するための trait です。
  *
  * @tparam T このトークナイザが生成するトークン
  * @tparam N このトーク内座が生成する構文ノード
  */
trait RelativeTokenizer[T <: Token, N <: Syntax.Node[T]] {

  /**
    * 指定された文章を係り受け解析しトークンに分割します。
    *
    * @param id        文章のID
    * @param paragraph 係り受け解析する文章
    * @return 解析された文章
    */
  def tokenize(id:Int, paragraph:String):RelativeDocument[T]

}
