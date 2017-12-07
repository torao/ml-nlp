/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.nlp.tools

import at.hazm.ml.nlp.model.Morph

class MorphNotFoundException(val morphs:Seq[Morph]) extends Exception(s"形態素が定義されていません: ${morphs.map(_.key).mkString(", ")}"){

}
