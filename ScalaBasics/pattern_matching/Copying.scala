package com.tekcrux.pattern_matching

object Copying {
  def main(args: Array[String]) {
    
    case class Message(sender: String, recipient: String, body: String)
    
    val message4 = Message("julien@bretagne.fr", "travis@washington.us", "Me zo o komz gant ma amezeg")
    val message5 = message4.copy(sender = message4.recipient, recipient = "claire@bourgogne.fr")
    
    println( message5.sender )          // travis@washington.us
    println(  message5.recipient )    // claire@bourgogne.fr
    println( message5.body )             // "Me zo o komz gant ma amezeg"
  }
}