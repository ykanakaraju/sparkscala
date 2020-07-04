package com.tekcrux.traits

trait Logger3 {
  def log(msg: String) { }
}

trait ConsoleLogger3 extends Logger3 {
  override def log(msg: String) { println(msg) }
}

trait AnotherLogger3 extends Logger3 {
  override def log(msg: String) { println("from AnotherLogger3: " + msg) }
}

class Account3 {
  protected var balance = 100.0
}

class SavingsAccount3 extends Account3 with Logger3 {
  def withdraw(amount: Double) {
    if (amount > balance) log("Insufficient funds")
    else{ balance -= amount; log("Funds withdrawn") }
  }

  // More methods ...
}

object ConsoleLogger3Mixin extends App {
  //val acct1 = new SavingsAccount3
  //acct1.withdraw(100) // No log message is printed as Logger3 trait is still abstract 

  //println("Overdrawing acct2");
  val acct2 = new SavingsAccount3 with ConsoleLogger3 with AnotherLogger3   // by mixing-in a ConcreteLogger logging is done here. 
  acct2.withdraw(200)
  
  /*println("Overdrawing acct3");
  val acct3 = new SavingsAccount3 with AnotherLogger3   // mixing-in with a different Logger 
  acct2.withdraw(50)*/
}