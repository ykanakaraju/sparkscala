package com.tekcrux.maps_tuples

object TuplesBasic extends App {
  
    val tuple3 = (1, 3.14, "Raju")
    printTuple(tuple3.productIterator, 1)
    println(tuple3.getClass)
    
    val tuple4 = (1, 3.14, "Raju", true)
    printTuple(tuple4.productIterator, 2)
    println(tuple4.getClass)
    
    val (first, second, third) = tuple3
    println("3 --> " + first + "/" + second + "/" + third)
    println("4 --> " + tuple3._1 + "/" + tuple3._2 + "/" + tuple3._3)
    
    //zip method
    val symbols = Array("<", "-", ">")
    val counts = Array(2, 10, 2)
    val pairs = symbols.zip(counts)
    printTuple(pairs.iterator, 5)
    for ((s, n) <- pairs) Console.print(s * n)
    println()
    
    
    
    def printTuple(x: Iterator[_], i:Int) {
       println(x.mkString(i + " --> " + x.getClass.getSimpleName + " (", ", ", ")"))
    } 
    
}


 