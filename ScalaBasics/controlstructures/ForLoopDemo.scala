package com.tekcrux.controlstructures

object ForLoopDemo extends App {
  
  var a = 0;
      
  // for loop execution with a range
  for( a <- 1 to 10 ){
      println( "Value of a: " + a );
  }

  val numList = List(1,2,3,4,5,6,7,8,9,10);

  println ("======= ex: 2 ==========")
  for( a <- numList ){
     println( "Value of a: " + a );
  }
  
  println ("======= ex: 3 ==========")
  for( a <- numList if a != 3; if a < 8 ){
         println( "Value of a: " + a );
  }
  
  println ("======= ex: 4 ==========")
  for( a <- 1 to 4; if (a > 1); b <- numList if a != 3; if a < 8 ){
         println( s"Value of $a*$b: $a*$b");
  }
}