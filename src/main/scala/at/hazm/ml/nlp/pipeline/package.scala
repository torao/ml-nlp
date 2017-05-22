package at.hazm.ml.nlp

package object pipeline {

  /**
    * 指定された文字列を正規化します。
    *
    * @see [[Text.normalize()]]
    */
  def Normalize() = IntakeFilter(Text.normalize)

  def Tokenize() = IntakeFilter({ t:String => Token.parse(t) })

}
