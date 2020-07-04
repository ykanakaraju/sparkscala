package com.tekcrux.collections

/*  Methods: zip  */

object ImpMethods3 extends App {
    val prices = List(15.0, 34.0, 19.5)
    val quantities = List(14, 12, 51)
    
    val zip1 = prices.zip(quantities)
    println(zip1)
    //printIterator(zip1.iterator, 1); println
    
    val zip2 = (prices zip quantities) map { p => p._1 * p._2 }    
    println(zip2)
    //printIterator(zip2.iterator, 2); println
    
    
    val zipSum = zip2.sum       //((prices zip quantities) map { p => p._1 * p._2 }) sum
    println("zipSum = " + zipSum); 
    
    val zip4 = List(5.0, 20.0, 9.95) zip List(10, 2)   //
    println(zip4)
    //printIterator(zip4.iterator, 4); println
    
    val zip5 = List(5.0, 20.0).zipAll(List(10, 1, 8, 9), 100, 101)   // 100 : default value for first list, 101 for seond list
    val zip6 = List(5.0, 20.0, 9.95).zipAll(List(10, 1), 100, 101)
    println(zip5)
    println(zip6)
    //printIterator(zip5.iterator, 5); println
    
    println( "Scala".zipWithIndex )
    
    println( "Scala".zipWithIndex.max )      // index of element with max encoding value
    
    println( "Scala".zipWithIndex.max._2 )
    
    //procedure ...
    def printIterator(col: Iterator[_], i: Int) {
       print("\n" + i + " --> ")
       print(col.mkString("<", ", ", ">"))    // mkSring converts an array into a string         
    }
}