package com.tekcrux.pattern_matching

object PatternMatch1 extends App {
    var sign = 0
    for (ch <- "+-!") {
        ch match {
          case '+' => sign = 1
          case '-' => sign = -1
          case _ => sign = 0
        }
        println(sign)
    }
    
    // assigning the match-case to a variable
    for (ch <- "+-!") {
        sign = ch match {
          case '+' => 1
          case '-' => -1
          case _ => 0
        }
        println(sign)
    }    
    
    // you can use type within case..
    import java.awt._
    val color = SystemColor.textText 
    val txt = color match {
      case Color.RED => "Text is red"
      case Color.BLACK => "Text is black"
      case _ => "Not red or black"
    }
    println(txt)
    println("-----------------------------")
    
    
    // Using guards inside case ..
    for (ch <- "+-3!") {
        var sign = 0
        var digit = 0
      
        ch match {
          case '+' => sign = 1
          case '-' => sign = -1
          case _ if Character.isDigit(ch) => digit = Character.digit(ch, 10)
          case _ => sign = 0
        }
      
        println(ch + " " + sign + " " + digit)
    }
    println("-----------------------------")
    
    // Using variables in case.. we are matching for str(i) which is assigned to ch 
    var str = "+-3!"
    for (i <- str.indices) {
        var sign = 0
        var digit = 0
      
        str(i) match {
          case '+' => sign = 1
          case '-' => sign = -1
          case ch if Character.isDigit(ch) => digit = Character.digit(ch, 10)
          case _ => 
        }
      
        println(str(i) + " " + sign + " " + digit)
    }
    
    
    
}