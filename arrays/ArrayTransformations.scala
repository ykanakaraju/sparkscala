package com.tekcrux.arrays

import scala.collection.mutable.ArrayBuffer

object ArrayTransformations extends App {
  
    val arr = Array(3, 5, 4, 7, 6, 9, 8, 10, 13, 12)
    val arrBuf = Array("s", "c", "a", "l", "a", "s", "p", "a", "r", "k")
    val arrWords = Array("mary", "had", "a", "little", "lamb")
    
    // yield produces a transformed Array
    val resultArr1 = for (elem <- arr) yield 2 * elem
    val resultArr2 = for (elem <- arr if elem % 2 == 0) yield elem   
    //arr.filter(_ % 2 == 0).map(2 * _)
    val sumArr = arr.sum  
    val maxArr = arr.max
    val minArr = arr.min   
    val countArr = arr.count(_ => true)
    val countArr2 = arr.count(_ >= 10)
    val sortedArr = arr.sorted
    
    def myComparator = (x: Int, y: Int) => x > y
    val sortedArr2 = arr.sortWith(_ > _)
    
    val sortedWords = arrWords.sortWith(_.length > _.length)
    //val sortedWords2 = arrWords.sortBy(_.length)
      
    printArray(resultArr1, 1)
    printArray(resultArr2, 2)
    println("\n3 --> sum: " + sumArr);
    println("4 --> max: " + maxArr);
    println("5 --> min: " + minArr);
    println("6 --> count: " + countArr);
    println("7 --> count (>=10): " + countArr2);
    printArray(sortedArr, 7)
    printArray(sortedArr2, 8)
    printArray(sortedWords, 9)
    

    
    //procedure ...
    def printArray(arr: Array[_], i: Int) {
       print("\n" + i + " --> ")
       print(arr.mkString("<", ", ", ">"))      
    }    
}


