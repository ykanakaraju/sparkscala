package com.tekcrux.maps_tuples

 import util.Random
 
object MinMax extends App { 

    val arr = randomArray(8)
    
    printMap(arr)
    
    val res = minMax(arr)
    
    println(res)
    
    def randomArray(n: Int) = {
    	val a = new Array[Int](n);
    	for (i <- 0 until a.size) a(i) = Random.nextInt(n)
    	a
    }
    
    def minMax(values: Array[Int]) = {
    	(values.min, values.max)
    }
    
    def printMap(x: TraversableOnce[_]) {
      println(x.mkString(x.getClass.getSimpleName + " (", ", ", ")"))
    } 
}