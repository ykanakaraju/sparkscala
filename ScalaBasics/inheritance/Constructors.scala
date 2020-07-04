package com.tekcrux.inheritance

object Constructors extends App {
  var kr = new Employee3("Kanakaraju", 42, 100000)
  print(kr.toString())
}

class Person3(val name: String, val age: Int) {
    println("Person3 primary constructor: [name=" + name + ", age = " + age + "]")
    override def toString = getClass.getName + "[name=" + name + ", age=" + age + "]"
}

class Employee3(name: String, age: Int, val salary : Double) extends  Person3(name, age) {
    println("Employee3 primary constructor: [name = " + name + ", age=" + age + ", salary = " + salary + "]")
    override def toString = super.toString + "[salary=" + salary + "]"
}