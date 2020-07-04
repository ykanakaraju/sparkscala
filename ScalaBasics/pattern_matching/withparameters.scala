package com.tekcrux.pattern_matching

object withparameters {
   def main(args: Array[String]) {
  case class Message(sender: String, recipient: String, body: String)
val message1 = Message("guillaume@quebec.ca", "jorge@catalonia.es", "Ça va ?")

println(message1.sender)  // prints guillaume@quebec.ca
//message1.sender = "travis@washington.us"  // this line does not compile
}}