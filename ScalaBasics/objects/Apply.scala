package com.tekcrux.objects

import scala.collection.mutable.{Map, Set}
import scala.collection.mutable._

object Apply extends App {
    
    val acct1 = Account1(10000.0);    
    println(acct1.description)    
    
    //val accountsArr = new Array[Account1](3)
    val accountsArr = Array(Account1(1500.0), Account1(1700.0), Account1(1200.0))
    for (x <- accountsArr) println( x.description )
  
}

class Account1 (val id: Int, initialBalance: Double) {
    
    private var balance = initialBalance
    
    def deposit(amount: Double) { balance += amount }
    def description = "Account " + id + " with balance " + balance
}

object Account1 { 
    def apply(initialBalance: Double) = {
         println("invoking Account1.apply method")
         new Account1(newUniqueNumber(), initialBalance)
    }
    private var lastNumber = 0
    private def newUniqueNumber() = { lastNumber += 1; lastNumber }
}













