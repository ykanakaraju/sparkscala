package com.tekcrux.maps_tuples

import scala.collection.immutable._

object MapsBasic extends App {
  
  val weekDays = Map("mon" -> 1, "mon" -> 1, "tue" -> 3, "wed" -> 4)    // Or  Map(("sun", 1), ("mon", 2), ("tue", 3))  
  val weekDaysMutable = scala.collection.mutable.Map("sun" -> 1, "mon" -> 2, "tue" -> 3) 
  
  printMap(weekDays, 1)
  printMap(weekDaysMutable, 2)
    
  //val wed = if (weekDaysMutable.contains("wed")) weekDaysMutable("wed") else "Wednesday does not exists yet"
  val wedMutable = weekDaysMutable.getOrElse("wed", "Wednesday does not exists yet")  
  println("\n3 --> " + wedMutable)
  
  weekDaysMutable("wed") = 4                          // adding an elements to mutable map
  val wedMutable2 = weekDaysMutable.getOrElse("wed", "Wednesday does not exists yet") 
  println(wedMutable2)
  weekDaysMutable += ("junk1" -> 100, "junk2" -> 101) // adding multiple elements
  printMap(weekDaysMutable, 4)
  
  weekDaysMutable("junk1") = 1001                     // updating en existing element
  printMap(weekDaysMutable, 5)
  
  // to remove a single element use weekDaysMutable -= "junk1"
  weekDaysMutable -= ("junk1", "junk2")               // remove elements. 
  printMap(weekDaysMutable, 6)
  
  
  //val keySet:Set[_] = weekDays.keySet   
  printMap(weekDays.keySet, 7)                        // extract keys from the Map
  printMap(weekDaysMutable.keySet, 8)
  printMap(weekDays.values, 9)                        // extract values from the map
  printMap(for ((k, v) <- weekDays) yield (v, k)  , 10)  // swap keys and values
  
    
  def printMap(x: TraversableOnce[_], i:Int) {
    println(x.mkString(i + " --> " + x.getClass.getSimpleName + " (", ", ", ")"))
  }  
  
}