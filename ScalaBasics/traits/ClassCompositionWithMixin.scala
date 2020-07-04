package com.tekcrux.traits

object ClassCompositionWithMixin extends App {
  
  //Example 1
  abstract class A {
    val message: String
  }
  
  class B extends A {
    val message = "I'm an instance of class B"
  }
  
  trait C extends A {
    def loudMessage = message.toUpperCase()
  }
  
  class D extends B with C
  
  val d = new D
  println(d.message)  // I'm an instance of class B
  println(d.loudMessage)  // I'M AN INSTANCE OF CLASS B
  
  
  //Example 2
  println("=== Example 2 ===")
  abstract class AbsIterator {
    type T
    def hasNext: Boolean
    def next(): T
  }
  
  
  class StringIterator(s: String) extends AbsIterator {
    type T = Char
    private var i = 0
    def hasNext = i < s.length
    def next() = {
      val ch = s charAt i
      i += 1
      ch
    }
  }
  
  trait RichIterator extends AbsIterator {
    def foreach(f: T => Unit): Unit = while (hasNext) f(next())
  }
  
  class RichStringIter extends StringIterator("Scala") with RichIterator
  val richStringIter = new RichStringIter
  richStringIter.foreach(println)
    
}