package com.tekcrux.pattern_matching

object CaseClassesComparision {
    def main(args: Array[String]) {
    case class Message(sender: String, recipient: String, body: String)

    val msg1 = Message("raju@gmail.com", "veer@tekcrux.com", "Lets meet for lunch tommorrow")
    val msg2 = Message("raju@gmail.com", "veer@tekcrux.com", "Lets meet for lunch tommorrow")
    println(msg1 == msg2)  // true
    
    val msg3 = msg2.copy(sender = msg2.recipient, recipient = "harsha@tekcrux.com")
    println(msg3.body)
    println(msg3.recipient)
    println(msg3.sender)
}}