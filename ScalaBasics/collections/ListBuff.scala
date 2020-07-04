package com.tekcrux.collections

import scala.collection.mutable.ListBuffer

object ListBuff extends App {
  
    println("1: Initialize a ListBuffer")
    
    val listBuffer1: ListBuffer[String] = ListBuffer("Kanakaraju","Veer","Harsha")
    
    println(s"Elements of listBuffer1 = $listBuffer1")
    
    println("\n2: Access elements at specific index in a ListBuffer")
    println(s"Element at index 1 = ${listBuffer1(1)}")
    
    println("\n3: Add elements to a ListBuffer using +=")
    listBuffer1 += "Munna"   // Use += to add & -= to remove element from ListBuffer
    listBuffer1 += ("Aditya", "Amrita")
    println(s"4: Elements of listBuffer1 = $listBuffer1")
    
    listBuffer1 -= ("Munna", "Amrita")
    println(s"5: Elements of listBuffer1 = $listBuffer1")
    
    val emptyListBuffer: ListBuffer[String] = ListBuffer.empty[String]
    println(s"6: Empty list buffer = $emptyListBuffer")
    
    listBuffer1.foreach{println}  
}