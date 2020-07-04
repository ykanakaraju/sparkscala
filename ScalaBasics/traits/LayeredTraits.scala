package com.tekcrux.traits

object LayeredTraits extends App {
    //val acct1 = new SavingsAccount4 with ConsoleLogger4 with TimestampLogger4 with ShortLogger4
    val acct2 = new SavingsAccount4 with ConsoleLogger4 with ShortLogger4 with TimestampLogger4
    //acct1.withdraw(48) 
    acct2.withdraw(48) 
}

trait Logger4 {
   def log(msg: String) { }
}


trait ConsoleLogger4 extends Logger4 {
   override def log(msg: String) { println(msg) }
}

trait TimestampLogger4 extends Logger4 {  
   override def log(msg: String) {
    super.log(new java.util.Date() + " " + msg)    
  }
}

trait ShortLogger4 extends Logger4 {
  val maxLength = 17 
  override def log(msg: String) {
    super.log(
      if (msg.length <= maxLength) msg 
      else msg.substring(0, maxLength - 3) + "...")
  }
}

class Account4 {
  protected var balance = 0.0
}

class SavingsAccount4 extends Account4 with Logger4 with ShortLogger4 {
  def withdraw(amount: Double) {
    if (amount > balance) log("Not enough funds..!")
    else balance -= amount
  }
}











