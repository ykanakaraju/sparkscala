package com.tekcrux.collections

object MutableImmutable extends App {
  
    val immutableMap: collection.immutable.Map[String, Int] = Map("Hello" -> 42) // Nobody can change it
    printCollection(immutableMap.iterator, 1);
    
    val mutableMap = new collection.mutable.HashMap[String, Int]
    printCollection(mutableMap.iterator, 2);
    
    val map: collection.Map[String, Int] = mutableMap    // defining an immutable map
    printCollection(map.iterator, 3);
    
    mutableMap.put("Kanakaraju", 17)
    // map.put("Fred", 29) // Error - can put into immutable map
    
    import scala.collection.mutable
    
    val immutableMap2 = Map("Hello" -> 42) 
    
    val mutableMap2 = new mutable.HashMap[String, Int]
    
    def digits(n: Int): Set[Int] =  if (n < 0) { digits(-n) } 
                                    else if (n < 10) { Set(n) } 
                                    else digits(n / 10) + (n % 10)
    
    var set: Set[Int] = digits(1729)
    println(set)

    def printCollection(x: Iterator[Any], i:Int) {
       println(x.mkString(i + " --> " + x.getClass.getSimpleName + " (", ", ", ")"))
    } 

}