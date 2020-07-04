package com.tekcrux.higherorderfunctions

object reduceRight extends App {
  val a = (1 to 10).reduceRight(_ - _)
    println(s"a = $a")
    
    val b = List(1,2,3,4,5,6).reduceRight((a, b) => a * b)
    println(s"b = $b")
    
    val c = List("Adam", "Bob", "Charlie", "David").reduceRight( (a, b) => if (a.length > b.length) a else b )
    println(s"c = $c")
}