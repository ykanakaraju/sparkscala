package com.tekcrux.basics

object LazyValues extends App {
  
    val nonLazyVal = {
        println("<------ initializing nonLazyVal -------> ")
        "This is nonLazyVal"
    }      
    
    lazy val lazyVal = {
        println("<------ initializing lazyVal -------> ")
        "This is lazyVal"
    }    
  
    println("<--- nothing is referenced yet --->")
    println("")
    println("nonLazyVal first time: " + nonLazyVal)
    println("")
    println("nonLazyVal second time: " + nonLazyVal)
    println("")
          
    println("lazyVal first time: " + lazyVal)
    println("")
    println("lazyVal second time: " + lazyVal) 
}