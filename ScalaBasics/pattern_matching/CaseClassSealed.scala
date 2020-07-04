package com.tekcrux.pattern_matching

sealed abstract class Amount3

case class Dollar3 (value: Double) extends Amount3
case class Currency3 (value: Double, unit: String) extends Amount3
case class Euro3(value: Double) extends Amount3

object CaseClassSealed extends App {
    def show(amt: Amount3) = 
      // You will get a "match not exhaustive" warning. 
      // That warning is not there if Amount isn't sealed.
      amt match { 
        case Currency3(v, u) => println( "I have " + v + " " + u )
        case _ => println("Others")
      }  
  
    println(show(Currency3(100000, "JPY")))
}