package com.tekcrux.arrays

import scala.collection.mutable.ArrayBuffer

object ArrayBuffersBasic extends App {
  
   val arrBuf = ArrayBuffer[Int]()
   
   arrBuf += 1                    // Add an element at the end with +=
   println("1 ==> " + arrBuf.toString())
   
   arrBuf += (2, 3, 5)            // Add multiple elements at the end by enclosing them in parentheses
   println("2 ==> " + arrBuf.toString())
   
   arrBuf ++= Array(8, 13, 21)    // You can append any collection with the ++= operator
   println("3 ==> " + arrBuf.toString())
   
   arrBuf.trimEnd(2)              // trimEnd(2) removes last 2 elements
   println("4 ==> " + arrBuf.toString())
   
   arrBuf.insert(2, 6)            // Insert element before index 2
   println("5 ==> " + arrBuf.toString())
   
   arrBuf.insert(2, 7, 8, 9)           // You can insert as many elements as you like
   println("6 ==> " + arrBuf.toString())
   
   arrBuf.remove(2)                    // removes element at index 2
   println("7 ==> " + arrBuf.toString())
   
   arrBuf.remove(2, 3)                 // The second parameter tells how many elements to remove from index 2
   println("8 ==> " + arrBuf.toString())
   
   // traversing ArrayBuffer is same as with Array
   print("\nTraversing ArrayBuffer\n9 ==> ")
   for (i <- 0 until arrBuf.length) print(arrBuf(i) + " ")
   
}