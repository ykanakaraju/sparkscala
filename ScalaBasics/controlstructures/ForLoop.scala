package com.tekcrux.controlstructures

object ForLoop extends App {
  
    var n = 4; var r = 0;
  
    for (i <- 1 to n) r = r + i    // (i <- 1 to n) returns a Range collection that is traversed in the loop
    println(s"r = $r") 
    
    var s = "scala"; var sum = ""
    for (i <- 0 until s.length) sum += s(i).toUpper + " "       // Last value for i is s.length - 1
    //for (i <- "scala") sum += i.toUpper + " "
    
    println(s"\nsum = $sum") 
    
    println("")
    for (i <- 1 to 3; j <- 1 to 3) print((10 * i + j) + " ")   // for loop with multiple conditions
    
    println("\n")
    for (i <- 1 to 3; j <- 1 to 3 if i != j) print((10 * i + j) + " ") // for loop with guard
    
    println("\n")
    for (i <- 1 to 3; from = 4 - i; j <- from to 3) print((10 * i + j) + " ")
    
    // for comprehension
    val str1 = for (c <- "Hello"; i <- 0 to 1) yield (c + i).toChar
    println("\n\n" + str1)
    
    val str2 = for (i <- 0 to 1; c <- "Hello") yield (c + i).toChar
    println("\n" + str2)
    
    val lst1 = for (c <- 1 to 5; i <- 0 to 3) yield (c * i)
    println("\n\n" + lst1)

}