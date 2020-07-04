package com.tekcrux.traits

object TraitExtendingClass extends App {
    val unhappyEx = new UnhappyException
    println( unhappyEx.getMessage() )
}

trait Logger {
    def log(msg: String)
}

trait ConsoleLogger extends Logger {
    def log(msg: String) { println(msg) }
}

//trait extending a class Exception
trait LoggedException extends Exception with ConsoleLogger {
    def log() { log(getMessage()) }    // getMessage() method is inherited from Exception class
}

// This class extends a trait. 
// Here both LoggedException & Exception becomes the superclasses of UnhappyException
class UnhappyException extends LoggedException { 
  override def getMessage() = "Message from UnhappyException"
}