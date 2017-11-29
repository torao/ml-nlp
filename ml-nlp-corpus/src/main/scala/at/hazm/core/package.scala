package at.hazm

import org.w3c.dom.{Element, Node, NodeList}

package object core {

  implicit class _Int(i:Int) {
    def times(f: => Unit):Unit = (0 until i).foreach { _ => f }
  }

  object XML {

    implicit class _NodeList(nl:NodeList) {
      def asScala:List[Node] = (for(i <- 0 until nl.getLength) yield nl.item(i)).toList

      def toElements:List[Element] = asScala.collect { case e:Element => e }
    }

  }

}
