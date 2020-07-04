package com.tekcrux.arrays

object MultiDimArrays extends App {
  
    val triangle = new Array[Array[Int]](10)
    
    for (i <- triangle.indices) triangle(i) = new Array[Int](i + 1)
    
    triangle(0)(0) = 1
    
    for (i <- 1 until triangle.length) {
        triangle(i)(0) = 1
        triangle(i)(i) = 1
        for (j <- 1 until triangle(i).length - 1) {
          triangle(i)(j) = triangle(i - 1)(j - 1) + triangle(i - 1)(j)
        }
    }
    
    for (row <- triangle) {
      for (elem <- row) print(elem + " ")      
      println()  
    }
}