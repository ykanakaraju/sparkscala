package com.tekcrux.pattern_matching

abstract class Amount
case class Dollar(value: Double) extends Amount
case class Currency(value: Double, unit: String) extends Amount

case object Nothing extends Amount
    
object CaseClasses extends App {   
    for (amt <- Array(Dollar(1000.0), Currency(1000.0, "EUR"), Nothing)) {
        val result = amt match {
           case Dollar(v) => "Dollars: " + v
           case Currency(_, u) => u
           case Nothing => "No Match"
        }
        // Note that amt is printed nicely, thanks to the generated toString
        println(amt + ": " + result)
    } 
    
    println("------------------------------")
    val amt = Currency(29.95, "EUR")
    val price = amt.copy(value = 19.95)
    println(price)
    println(amt.copy(unit = "CHF"))
}