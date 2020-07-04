package com.tekcrux.objects

/*
object AppTrait {
  def main(args: Array[String]) {
     if (args.length > 0) {
        print("The following arguments are supplied: ")
        println(args.mkString(","))
        println(args.reverse.mkString(","))
     }
     else{
       println("No arguments are supplied")
     }
  }
} 
*/

object AppTrait2 extends App {  
   if (args.length > 0) {
      print("The following arguments are supplied: ")
      println(args.mkString(","))
      println(args.reverse.mkString(","))
   }
   else{
     println("No arguments are supplied")
   }  
}