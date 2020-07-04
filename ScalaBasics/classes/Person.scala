package com.tekcrux.classes

object SomePerson extends App {  
  
    val raju = new Person
         
    raju.age = 40    // calling a set method
    println(raju.age)
    
    raju.age = 45   // you are calling age_ method
    println(raju.age)  
  
}

class Person {
   private var privateAge = 0 // Make private and rename
   
   // getter method or accessor
   def age = privateAge
   
   // setter method or mutator
   def age_= (newValue: Int) {
      if (newValue > privateAge) privateAge = newValue; 
   }  
   
   def setAge(newValue: Int) {
     if (newValue > privateAge) privateAge = newValue; 
   }
}


