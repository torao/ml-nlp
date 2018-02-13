/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements; and to You under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package at.hazm.core.io

import java.io.{IOException, PushbackReader, Reader}

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * 入力ストリームから CSV または TSV データを読み込んでレコードごとに列挙します。
  * このクラスではストリームのクローズは行いません。
  *
  * @param in        CSV/TSV を読み込む入力ストリーム
  * @param separator フィールドセパレータ文字
  */
class CSVIterator(in:Reader, separator:Char = ',') extends Iterator[Seq[String]] {

  /**
    * 実際に使用する入力ストリーム。
    */
  private[this] val _in = new PushbackReader(in)

  /**
    * 次の参照で返されるレコード。
    */
  private[this] var nextFields:Option[Seq[String]] = prefetch()

  /**
    * この列挙に次のレコードが存在する場合に true を返します。
    *
    * @return 次のレコードが存在する場合 true
    */
  override def hasNext:Boolean = nextFields.isDefined

  /**
    * この列挙から次のレコードを参照します。
    *
    * @return 次のレコード
    */
  override def next():Seq[String] = {
    val fields = nextFields.get
    nextFields = prefetch()
    fields
  }

  /**
    * 入力ストリームから次のレコードを読み込んで返します。
    *
    * @return 次のレコード
    */
  private[this] def prefetch():Option[Seq[String]] = _prefetch(new StringBuilder(), mutable.Buffer[String](), quoted = false).map { fields =>
    fields.map {
      case field if field.length >= 2 && field.head == '\"' && field.charAt(field.length - 1) == '\"' =>
        field.substring(1, field.length - 1).replace("\"\"", "\"")
      case field => field
    }
  }

  /**
    * 入力ストリームから次のレコードを読み込んで返します。返値の文字列シーケンスは引用記号が含まれています。
    *
    * @param field  フィールド文字列用のバッファ
    * @param fields レコードを表すフィールドの並び用バッファ
    * @param quoted 引用記号内を処理中の場合 true
    * @return 読み込んだ引用記号付きレコード
    */
  @tailrec
  private[this] def _prefetch(field:StringBuilder, fields:mutable.Buffer[String], quoted:Boolean):Option[Seq[String]] = _in.read() match {
    case ch if quoted =>
      if(ch == '\"') {
        _in.read() match {
          case '\"' =>
            field.append("\"\"")
            _prefetch(field, fields, quoted = true)
          case trailer =>
            field.append("\"")
            if(trailer >= 0) {
              _in.unread(trailer)
            }
            _prefetch(field, fields, quoted = false)
        }
      } else if(ch < 0){
        val fragment = (fields.mkString(",").takeRight(15) + ",>\"<" + field.take(30) + "...").replaceAll("\\s", " ")
        throw new IOException(s"quoted field is not end: $fragment...")
      } else {
        field.append(ch.toChar)
        _prefetch(field, fields, quoted = true)
      }
    case '\n' =>
      Some(fields :+ field.toString())
    case '\r' =>
      _in.read() match {
        case '\n' => None
        case eof if eof < 0 => None
        case ch => _in.unread(ch)
      }
      Some(fields :+ field.toString())
    case eof if eof < 0 =>
      if(field.isEmpty && fields.isEmpty) None else Some(fields :+ field.toString())
    case ch if ch == separator =>
      fields.append(field.toString())
      field.setLength(0)
      _prefetch(field, fields, quoted = false)
    case ch =>
      field.append(ch.toChar)
      _prefetch(field, fields, quoted = field.length == 1 && ch == '\"')
  }
}
