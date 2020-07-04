package com.tekcrux.traits

trait Logged8 {
  def log(msg: String) { }
}

trait ConsoleLogger8 extends Logged8 {
  override def log(msg: String) { println(msg) }
}

trait ShortLogger8 extends Logged8 {
  val maxLength: Int // An abstract field
  override def log(msg: String) {
    super.log(
      if (msg.length <= maxLength) msg else msg.substring(0, maxLength - 3) + "...")
  }
}

class Account8 {
  protected var balance = 150.0
}

class SavingsAccount8 extends Account8 with Logged8 {
  var interest = 0.0
  def withdraw(amount: Double) {
    if (amount > balance) log("Insufficient funds. You account balance is " + balance)
    else{
         balance -= amount
         log(amount + " debited from your account. Your Current balance is " + balance)
      }
  }

  // More methods ...
}

object TraitsWithAbstractFields8 extends App {
  val acct = new SavingsAccount8 with ConsoleLogger8 with ShortLogger8 {
    val maxLength = 30   // You must initialize this abstract field. 
  }
  acct.withdraw(100) 
  
  val acct2 = new SavingsAccount8 with ConsoleLogger8 with ShortLogger8 {
    val maxLength = 50   // You must initialize this abstract field. 
  }
  acct2.withdraw(250) 
  
  
    // Log message is not truncated because maxLength is 20
}
