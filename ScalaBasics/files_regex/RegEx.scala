package com.tekcrux.files_regex

import scala.util.matching.Regex

object RegEx extends App {
  
    val testStr = "10 mangos, 12 bananas, 21 oranges, 6 apples, 1 papaya"
    val numPattern = "[0-9]+".r  
    val wsnumwsPattern = """\s+[0-9]+\s+""".r
    
    for (matchString <- wsnumwsPattern.findAllIn(testStr)) println("1 >> " + matchString)
    
    val firstMatch = wsnumwsPattern.findFirstIn(testStr)
    for (x <- firstMatch) println("2 >> " + x)
    
      
    val anchoredPattern = "^[0-9]+$".r
    val str = " 123"    // "123"
    if (anchoredPattern.findFirstIn(str) == None) println("Not a number")
    if (str.matches("[0-9]+")) println("A number")
    
    for(aa <- numPattern.replaceFirstIn(testStr, "XX")) print(aa)
    
    println()
    for(bb <- numPattern.replaceAllIn(testStr, "XX")) print(bb)
    
    println()
    for (cc <- numPattern.replaceSomeIn(testStr, m => if (m.matched.toInt % 2 == 0) Some("XX") else None)) print(cc)
    
    //val varPattern = """\$[0-9]+""".r
    
    //def format(message: String, vars: String*) = varPattern.replaceSomeIn(message, m => vars.lift(m.matched.tail.toInt))
    //format("At $1, there was $2 on $0.", "planet 7", "12:30 pm", "a disturbance of the force")
}