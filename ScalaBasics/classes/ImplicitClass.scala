package com.tekcrux.classes

object Helpers {
  implicit class IntWithTimes(x: Int) {
    def times[A](f: => A): Unit = {
      def loop(current: Int): Unit =
        if(current > 0) {
          f
          loop(current - 1)
        }
      loop(x)
    }
  }
  
  def main(args: Array[String]) {
     5 times println("Hello")
  }
}

/*
Some notes about Implicit Classes

An implicit class is a class marked with the implicit keyword. 
This keyword makes the class’s primary constructor available for 
implicit conversions when the class is in scope.


    1. Implicit Classes must be defined inside of another trait/class/object.
    
    	 	object Helpers {
           implicit class RichInt(x: Int) // OK!
        }
        implicit class RichDouble(x: Double) // BAD!

    2. They may only take one non-implicit argument in their constructor.
    
    	 	implicit class RichDate(date: java.util.Date) // OK!
        implicit class Indexer[T](collecton: Seq[T], index: Int) // BAD!
        implicit class Indexer[T](collecton: Seq[T])(implicit index: Index) // OK!

    3. There may not be any method, member or object in scope with the same name as the implicit class.
       This means an implicit class cannot be a case class.
       
       	object Bar
        implicit class Bar(x: Int) // BAD!
        
        val x = 5
        implicit class x(y: Int) // BAD!
        
        implicit case class Baz(x: Int) // BAD!    
*/