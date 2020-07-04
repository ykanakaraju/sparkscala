package com.tekcrux.basics

object FunctionsVariableArgs {
  
  def main(args: Array[String]) {
      printStrings("Hello", "Scala", "Python", "Java");
  }
   
  def printStrings( args: String* ) = {
      var i : Int = 0;
      
      for( arg <- args ){
         println(s"Arg value[$i] = $arg");
         i = i + 1;
      }
  }
}