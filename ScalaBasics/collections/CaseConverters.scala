package com.tekcrux.collections

object CaseConverters extends App {
  
    //case-converter using zip & map methods
    val upperCaseString = toUpperCase("TesT StrinG")
    println(upperCaseString)
    
    val lowerCaseString = toLowerCase("TesT StrinG")
    println(lowerCaseString)
        
    def toLowerCase(input: String) = {
       val upperToLower = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".zip("abcdefghijklmnopqrstuvwxyz").toMap
       input.map( x => upperToLower.getOrElse(x, x) )
    }
    
    def toUpperCase(input: String) = {
       val lowerToUpper = "abcdefghijklmnopqrstuvwxyz".zip("ABCDEFGHIJKLMNOPQRSTUVWXYZ").toMap
       input.map( x => lowerToUpper.getOrElse(x, x) )
    }
}