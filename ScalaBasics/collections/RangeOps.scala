package com.tekcrux.collections

object RangeOps extends App {
  
  /*  scala> 1 to 10
      res0: scala.collection.immutable.Range.Inclusive = Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      
      scala> 1 until 10
      res1: scala.collection.immutable.Range = Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
      
      scala> 1 to 10 by 2
      res2: scala.collection.immutable.Range = Range(1, 3, 5, 7, 9)
      
      scala> 'a' to 'c'
      res3: collection.immutable.NumericRange.Inclusive[Char] = NumericRange(a, b, c)
   */
  
    val list1 = (1 to 20 by 3).toList
    println(list1);
    
    val list2 = ('a' to 'z' by 2).toList
    println(list2);
    
    val array1 = (1 until 10).toArray
    println(array1.mkString(","));
    
    val array2 = ('a' to 'm').toArray
    println(array2.mkString(","));
    
    val set1 = (1 to 10).toSet
    println(set1);
    
    val x1 = Array.range(1, 10)
    println(x1.getClass.getSimpleName + " >> " + x1.mkString(","));
    
    val x2 = Array.range('a', 'm')
    println(x2.getClass.getSimpleName + " >> " + x2.mkString(","));
  
    val x3 = Vector.range(1, 10)
    println(x3);
    
    val x4 = List.range(1, 10)
    println(x4);
    
    val x5 = List.range(0, 10, 2)
    println(x5);
    
    val x6 = collection.mutable.ArrayBuffer.range('a', 'd')
    println(x6);
    
    println("--------------------------------------")
    
    for (i <- 1 to 3) println(i)
    
    println("--------------------------------------")
     
    val i = (1 to 5).map { e => (e + 0.5) * 2 }.foreach(println)
    
    println("--------------------------------------")
     
     
}