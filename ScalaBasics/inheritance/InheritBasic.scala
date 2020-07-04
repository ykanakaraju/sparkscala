package com.tekcrux.inheritance

class Person2 {
  var name = ""
  override def toString = getClass.getName + "[name=" + name + "]"
}

class Employee2 extends Person2 {
  var salary = 0.0
  override def toString = super.toString + "[salary=" + salary + "]"
}

class Manager2 extends Employee2

object Main2 extends App {
    val r = scala.math.random
    
    println("Random value of r: " + r)
    
    val p = if (r < 0.33) new Person2
            else if (r < 0.67) new Employee2 
            else new Manager2  
      
    if (p.isInstanceOf[Employee2]) {     
        val s = p.asInstanceOf[Employee2] // s has type Employee
        println("It's an employee.")
        s.salary = 50000
        
        if (p.getClass == classOf[Manager2]) {
            println("Actually, it's a manager")
            s.salary *= 2
        }
    }
    
    println(p)
}