package com.tekcrux.collections

object CommonMethods extends App {
  
    val coll = Range(1, 10)
    coll.head  
    println(coll.head)    
    coll.tail  
    println(coll.tail)
    println(coll.tail.head)
    println(coll.tail.tail)
    
    println(coll.last)
    println(coll.headOption)
    println(coll.lastOption)
    
    coll.length
    val isEmpty = coll.isEmpty
    
    coll.sum
    coll.product
    coll.max
    coll.min
    
    coll.count(_ % 2 == 0)
    coll.forall(_ % 2 == 0)
    coll.exists(_ % 2 == 0)
    
    coll.filter(_ % 2 == 0)   //.foreach(println)
    
    
    coll.filterNot(_ % 2 == 0)
    coll.partition(_ % 2 == 0)
    
    coll.takeWhile(_ < 3)
    coll.dropWhile(_ < 3)
    coll.span(_ < 3)
    
    coll.take(4)
    coll.drop(4)
    coll.splitAt(4)
    
    coll.takeRight(4)
    coll.dropRight(4)
    
    coll.slice(2, 8)
    
    coll.grouped(3).toArray
    coll.sliding(3).toArray
    
    coll.mkString("<", "|", ">")
    
    
    coll.toIterable
    coll.toSeq
    coll.toIndexedSeq
    coll.toArray
    coll.toList
    coll.toSet
    
    // Seq methods    
    coll.indexWhere(_ % 3 == 0)
    
    coll.prefixLength(_ % 4 != 0)
    coll.segmentLength(_ % 4 != 0, 4)
    coll.padTo(20, 0)
    
    val a = Seq(1, 1, 2, 3, 1, 1, 1)
    val b = Seq(1, 2, 3, 2, 1)
    
    a intersect b
    a diff b
    
    val words = "Mary had a little lamb".split(" ")    
    //println(words.reverse.mkString(" "))    
    words.sorted
    words.sortWith(_.length < _.length)
    words.sortBy(_.length)
    
    words.permutations.toArray
    words.combinations(3).toArray
}