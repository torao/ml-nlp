/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 *  agreements; and to You under the Apache License, Version 2.0.
 *  http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.ml.neocortex.model

import java.io._
import java.nio.charset.{Charset, StandardCharsets}

import at.hazm.core.io.using
import at.hazm.ml.neocortex.model.DataStore.logger
import org.slf4j.LoggerFactory

class DataStore private[neocortex](_dir:File) {

  /**
    * ローカルファイルシステム上のデータ保存用ディレクトリ。
    */
  val dir:File = _dir.getAbsoluteFile

  /**
    * 指定されたファイルからバイナリ入力のためのストリームを参照します。
    *
    * @param file 入力元のファイル
    * @param f    入力処理
    * @tparam T 入力処理の結果
    * @return 入力処理の結果
    */
  def binaryFrom[T](file:File)(f:(InputStream) => T):T = using(new BufferedInputStream(new FileInputStream(file)))(f)

  /**
    * 指定されたファイルからバイナリ入力のためのストリームを参照します。
    *
    * @param file 入力元のファイル
    * @param f    入力処理
    * @tparam T 入力処理の結果
    * @return 入力処理の結果
    */
  def textFrom[T](file:File, charset:Charset = StandardCharsets.UTF_8)(f:(BufferedReader) => T):T = binaryFrom(file) { is =>
    val in = new BufferedReader(new InputStreamReader(is, charset))
    f(in)
  }

  /**
    * 指定されたファイルに対して安全に出力を行います。処理 f による出力が完了したあとに file へリネームされます。
    *
    * @param file 出力先のファイル
    * @param f    出力処理
    * @tparam T 出力処理の結果
    * @return 出力処理の結果
    */
  def writeTo[T](file:File)(f:(File) => T):T = {
    val dir = file.getAbsoluteFile.getParentFile
    val temp = File.createTempFile("~" + file.getName, ".tmp", dir)
    try {
      val result = f(temp)
      if(!temp.renameTo(file)) {
        if(!file.exists() || (file.delete() && !temp.renameTo(file))) {
          throw new IOException(s"fail to rename: ${temp.getAbsolutePath} -> ${file.getName}")
        }
      }
      result
    } catch {
      case ex:Throwable =>
        logger.error(s"fail to output: $file")
        temp.delete()
        throw ex
    }
  }

  /**
    * 指定されたファイルに対して安全に出力を行います。処理 f による出力が完了したあとに file へリネームされます。
    *
    * @param file 出力先のファイル
    * @param f    出力処理
    * @tparam T 出力処理の結果
    * @return 出力処理の結果
    */
  def binaryTo[T](file:File)(f:(OutputStream) => T):T = writeTo(file) { temp =>
    using(new BufferedOutputStream(new FileOutputStream(temp)))(f)
  }

  /**
    * 指定されたファイルに対して安全に出力を行います。処理 f による出力が完了したあとに file へリネームされます。
    *
    * @param file    出力先のファイル
    * @param charset 出力文字セット
    * @param f       出力処理
    * @tparam T 出力処理の結果
    * @return 出力処理の結果
    */
  def textTo[T](file:File, charset:Charset = StandardCharsets.UTF_8)(f:(PrintWriter) => T):T = binaryTo(file) { os =>
    val out = new PrintWriter(new OutputStreamWriter(os, charset))
    val result = f(out)
    out.flush()
    result
  }
}

object DataStore {
  private[DataStore] val logger = LoggerFactory.getLogger(classOf[DataStore])
}