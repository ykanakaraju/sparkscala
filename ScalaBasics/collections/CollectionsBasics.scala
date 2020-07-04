package com.tekcrux.collections

object CollectionsBasics extends App {
  
    val coll = Array(1, 7, 2, 9) // some Iterable
    val iter = coll.iterator
    while (iter.hasNext) println(iter.next())
    
    import java.awt.Color
    
    val iterable = Iterable(0xFF, 0xFF00, 0xFF0000)
    printCollection(iterable.iterator, 1)
    
    val set = Set(Color.RED, Color.GREEN, Color.BLUE)
    printCollection(set.iterator, 2)
    
    val map = Map(Color.RED -> 0xFF0000, Color.GREEN -> 0xFF00, Color.BLUE -> 0xFF)
    printCollection(map.iterator, 3)
            
    import scala.collection._
    
    val ss = SortedSet("Hello", "World")
    printCollection(ss.iterator, 4)
    
    def printCollection(x: Iterator[Any], i:Int) {
       println(x.mkString(i + " --> " + x.getClass.getSimpleName + " (", ", ", ")"))
    } 
    

}