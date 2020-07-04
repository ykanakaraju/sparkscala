package com.tekcrux.packages

package object people {
   val defaultName = "Kanakaraju Y"   
}

package people {
   class Person {
      var name = defaultName       // A constant from the package
      def description = "A person with name " + name
   } 
}

object Main extends App {
    val john = new com.tekcrux.packages.people.Person
    println(john.description)    // here we are accessing a method defined at package level 
}


