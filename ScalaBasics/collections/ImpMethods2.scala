package com.tekcrux.collections

/*  Methods: reduceLeft, reduceRight, foldLeft, scanLeft  */

object ImpMethods2 extends App {
  
    val reduceLeft = List(1, 7, 2, 9).reduceLeft(_ - _)      // (((1-7)-2)-9) = 1-7-2-9 = -17
    println("reduceLeft = " + reduceLeft);
  
    val reduceRight = List(1, 7, 2, 9).reduceRight(_ - _)    // (1-(7-(2-9))) = 1-7+2-9 = -13
    println("reduceRight = " + reduceRight);
    
    val foldLeft = List(1, 7, 2, 9).foldLeft(0)(_ - _)      // (((0-1)-7)-2)-9) = 0-1-7-2-9 = -19
    //(0 /: List(1, 7, 2, 9))(_ - _)
    println("foldLeft = " + foldLeft);
        
    val scanLeft = (1 to 3).scanLeft(1)(_ + _)    // 0, 0+1, 0+1+2,  0+1+2+3
    val scanRight = (1 to 3).scanRight(1)(_ + _)  // 0+3+2+1, 0+3+2, 0+3, 0   
    println("scanLeft = " + scanLeft);
    println("scanRight = " + scanRight);
    
    //procedure ...
    def printArray(arr: Iterator[_], i: Int) {
       print("\n" + i + " --> ")
       print(arr.mkString("<", ", ", ">"))    // mkSring converts an array into a string         
    }
    
    
    /*
    // Compute a map with frequency of letters in a word
    val freq = scala.collection.mutable.Map[Char, Int]()
    for (c <- "Mississippi") freq(c) = freq.getOrElse(c, 0) + 1
    
    (Map[Char, Int]() /: "Mississippi") {
      (m, c) => m + (c -> (m.getOrElse(c, 0) + 1))
    }
    */
  
}