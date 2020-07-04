package com.tekcrux.collections

object ListsBasic extends App {
  
    val digits = List(4, 2, 8)   // 4 :: 2 :: 8 :: Nil
        
    val digits2 = 9 :: digits    // add an element to a List - 9 :: (4 :: (2 :: (8 :: Nil)))
    println("1 >> " + digits2.mkString(",") )
    
    //recursive method to sum the digits of a number
    def sum1(lst: List[Int]): Int = if (lst == Nil) 0 else lst.head + sum1(lst.tail)
    
    val sumList = sum1(digits2)
    println("2 >> " + sumList)
        
    def sum2(lst: List[Int]): Int = lst match {
      case Nil => 0
      case h :: t => h + sum2(t) // h is lst.head, t is lst.tail
    }
    
    val sumList2 = digits2.sum  //sum2(digits2)
    println("3 >> " + sumList2)
    
    digits2.foreach(println)  // also digits2.foreach{println}
    
    //println(List(9, 4, 2).sum) 
    
    //List concatination
    val fruit1 = "apples" :: ("oranges" :: ("pears" :: Nil))
    val fruit2 = "mangoes" :: ("banana" :: Nil)

    // use two or more lists with ::: operator
    var fruit = fruit1 ::: fruit2
    println( "fruit1 ::: fruit2 : " + fruit )
      
    // use two lists with List.:::() method
    fruit = fruit1.:::(fruit2)
    println( "fruit1.:::(fruit2) : " + fruit )

    // pass two or more lists as arguments
    fruit = List.concat(fruit1, fruit2)
    println( "List.concat(fruit1, fruit2) : " + fruit  )
    
    //Creating Uniform Lists
    val pets = List.fill(3)("dog") // Repeats dog three times.
    println( "pets : " + pets  )

    val num = List.fill(10)(2)     // Repeats 2, 10 times.
    println( "num : " + num  )
}