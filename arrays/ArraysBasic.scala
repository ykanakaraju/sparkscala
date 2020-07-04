package com.tekcrux.arrays

object ArraysBasic extends App {
  
    val intArr = new Array[Int](5)
    for (i <- 0 until intArr.length) intArr(i) = i*i
    
    //for (i <- 0 until intArr.length) intArr(i) = i*2
    
    print("1 ===> ")
    for (i <- 0 until intArr.length) print(intArr(i) + " ")  
    
    print("\n2 ===> ")
    for (element <- intArr) print(element*element + " ")      
   
    // pre-initialized array. no new here..    
    val strArr2 = Array("Hello", "World")
    strArr2(0) = "Goodbye"
    
    for (i <- strArr2) print(i + " ")
}