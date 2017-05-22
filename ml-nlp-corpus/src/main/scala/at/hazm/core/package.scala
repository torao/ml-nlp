package at.hazm

package object core {

  implicit class _Int(i:Int){
    def times(f: =>Unit):Unit = (0 until i).foreach{ _ => f }
  }
}
