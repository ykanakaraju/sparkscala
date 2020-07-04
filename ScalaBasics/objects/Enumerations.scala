package com.tekcrux.objects

object Enumerations extends App {  
  println("Red: id: " + TrafficLightColor.Red.id + ", value: "  + TrafficLightColor.Red)
  println("Yellow: id: " + TrafficLightColor.Yellow.id + ", value: "  + TrafficLightColor.Yellow)
  println("Green: id: " + TrafficLightColor.Green.id + ", value: "  + TrafficLightColor.Green)
  
  println(TrafficLightColor(0).toString())   // stop
  println(TrafficLightColor.withName("Go").toString())
  println(TrafficLightColor.withName("Yellow").toString())      
  
  println("--------------------------------------");
  println("Spade: id: " + Suits.Spade.id + ", value: "  + Suits.Spade)
  println("Club: id: " + Suits.Club.id + ", value: "  + Suits.Club)
  println("Heart: id: " + Suits.Heart.id + ", value: "  + Suits.Heart)
  println("Diamond: id: " + Suits.Diamond.id + ", value: "  + Suits.Diamond)
  
  println("--------------------------------------");
  println(Suits.values); 
  
}


object Suits extends Enumeration {
	//type Suits = Value
	val Spade = Value(10, "♠")
	val Club = Value(20, "♣")
	val Heart = Value(30, "♥")
	val Diamond = Value(40, "♦")	
}

object TrafficLightColor extends Enumeration {
  val Red = Value(0, "Stop")  // id = 0, value= "Stop"
  val Yellow = Value(10)      // id =
  val Green = Value("Go")  
}






