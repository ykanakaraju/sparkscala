package com.tekcrux.pattern_matching

case class Currency2 (value: Double, unit: String)

object CaseClassInfix extends App {
  
    val amt = Currency2(1000.0, "EUR")
    
    amt match { case a Currency2 u => a + " " + u }
    
    val lst = List(1, 7, 2, 9)
    lst match { 
      case h :: t => println( h + " " + t.length )
      case _ => 0
    } 
    
    /*
    lst match { 
      case ::(h, t) => println( h + " " + t.length )
      case _ => 0
    }  
    */
        
    // :: is right-associative
    
    List(1, 7, 2, 9) match { 
      case first :: second :: rest => println(first  + " " +  second  + " " +  rest) 
      case _ => 0
    }
    
    List(1, 7, 2, 9) match { 
      case ::(first, ::(second, rest)) => println(first  + " " +  second  + " " +  rest.length) 
      case _ => 0
    }
    
    List(List(1, 7), List(2, 9)) match { 
      case (first :: second) :: rest => println(first  + " " +  second  + " " +  rest) 
      case _ => 0
    }
}