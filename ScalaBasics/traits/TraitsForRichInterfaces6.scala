package com.tekcrux.traits

trait Logger6 {
    def log(msg: String)
    def info(msg: String) { log("INFO: " + msg) }
    def warn(msg: String) { log("WARN: " + msg) }
    def fatal(msg: String) { log("BREACH: " + msg) }
}

class Account6 {
    protected var balance = 2000.0
    protected val minBalanceThreshold = 100.0
}

class SavingsAccount6 extends Account6 with Logger6 {
    def withdraw(amount: Double) {
      if (amount > balance){
        fatal("Insufficient funds. You account balance is " + balance)
      }
      else if (amount < balance && (balance - amount) < minBalanceThreshold) {
        balance -= amount
         warn(amount + " debited from your account. Current balance " + balance + " is below minimum required balance of " + minBalanceThreshold)
         
      }
      else{
         balance -= amount
         info(amount + " debited from your account. Your Current balance is " + balance)
      }
    }
    // More methods ...
    override def log(msg: String) { println(msg) }
}

object TraitsForRichInterfaces6 extends App {
    val acct1 = new SavingsAccount6
    acct1.withdraw(1000) 
    acct1.withdraw(950) 
    acct1.withdraw(100) 
}