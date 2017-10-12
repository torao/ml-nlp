package at.hazm.core.io

import java.io.{PushbackReader, Reader}

class LineNumberReader(r:Reader, pushBackSize:Int) extends PushbackReader(r, pushBackSize) {
  private[this] var _line = 0L
  private[this] var _column = 0L

  /** 直前の行のカラム数 */
  private[this] var _prevColumnSize = -1L

  /**
    * 現在まで読み込んだ行数を返します。先頭の行に対して 0 を返します。
    *
    * @return 読み込んだ行数
    */
  def line:Long = _line

  /**
    * 現在の行で読み込んだ文字数を返します。行の先頭に対して 0 を返します。unread() によって前の行をポイントした場合は
    * 一不定として負の値を返します。
    *
    * @return 行内で読み込んだ文字数
    */
  def column:Long = _column

  override def read():Int = _read(super.read())

  override def read(cbuf:Array[Char]):Int = {
    val len = super.read(cbuf)
    for(i <- 0 until len) _read(cbuf(i))
    len
  }

  override def read(cbuf:Array[Char], off:Int, l:Int):Int = {
    val len = super.read(cbuf, off, l)
    for(i <- 0 until len) _read(cbuf(off + i))
    len
  }

  override def unread(c:Int):Unit = {
    _unread(c)
    super.unread(c)
  }

  private[this] def _read(ch:Int):Int = {
    if(ch == '\n'){
      _line += 1
      _prevColumnSize = _column
      _column = 0
    } else if(_column >= 0) _column += 1
    ch
  }

  private[this] def _unread(ch:Int):Int = {
    if(ch == '\n') {
      _line -= 1
      _column = _prevColumnSize
      _prevColumnSize = -1
    } else if(_column >= 0) _column -= 1
    ch
  }
}
