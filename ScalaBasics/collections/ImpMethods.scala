package com.tekcrux.collections

/*  Methods: map, flatMap, collect, transform, groupBy  */

object ImpMethods extends App {
  
    val names = List("Kanakaraju", "Veer", "Nagaraju", "Harsha")
    val arrayBuffnames = scala.collection.mutable.ArrayBuffer("Kanakaraju", "Veer", "Nagaraju", "Harsha")
    
    // map example
    val upperNames = names.map(_.toUpperCase)
    printArray(upperNames.iterator, 1); println;   
    
    // map functionality with for comprehension
    val upperNames2 = for (n <- names) yield n.toUpperCase
    printArray(upperNames2.iterator, 2 ); println;  
        
    
    // flatMap Example - map Vs flatMap
    def ulcase(s: String) = Vector(s.toUpperCase(), s.toLowerCase())
    
    val mapUC = names.map(ulcase)    
    printArray(mapUC.iterator, 3); println; 
    
    val fMapUC = names.flatMap(ulcase)
    printArray(fMapUC.iterator, 4); println; 
    
    
    //transform example
    val trans = arrayBuffnames.transform( x => x.length().toString() )
    printArray(trans.iterator, 5); println; 
    
    //collect Example    
    val col = "-3+4".collect { case '+' => 1 ; case '-' => -1 }
    printArray(col.iterator, 6); println; 
    
    //groupBy Example
    val words = List("Spark", "Scala", "Python", "Java", "Hadoop", "HDFS", "MapReduce", "MongoDB")
    val grpByMap = words.groupBy(_.substring(0, 1).toUpperCase)
    printArray(grpByMap.iterator, 7); println; 
     
    //procedure ...
    def printArray(arr: Iterator[_], i: Int) {
       print("\n" + i + " --> ")
       print(arr.mkString("<", ", ", ">"))    // mkSring converts an array into a string         
    }
  
}