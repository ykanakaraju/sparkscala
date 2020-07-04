package com.tekcrux.inheritance

class Person {
    protected[this] var age = 0   // age is not accessible to subclasses, but not to objects of subclasses
            
    def setAge(newAge: Int) { // A person can never get younger
      if (newAge > age) age = newAge 
    }    
}

class Manager extends Person {
  
    protected var salary = 0.0
    
    def setSalary(newSalary: Double) { // A manager's salary can never decrease
      if (newSalary > salary) salary = newSalary 
    } 
    
    // Can access age from superclass
    def description = "a manager who is " + age + " years old and makes " + salary
  
    def isSeniorTo(other: Manager) = salary > other.salary 
      // Can't access age of another person. The following doesn't work:
      // age > other.age
}

object Main extends App {
  
    var veer = new Manager
    veer.setAge(50)
    veer.setSalary(150000)
    
    var raju = new Manager
    raju.setAge(40)
    raju.setSalary(100000)
    
    if (veer.isSeniorTo(raju)) 
      println(veer.description + "\nis senior to " + raju.description)
    else
      println(raju.description + "\nis senior to " + veer.description)
}