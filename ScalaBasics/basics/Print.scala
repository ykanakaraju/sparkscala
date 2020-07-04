package com.tekcrux.basics

object PrintExample extends App{
  
  val name = "Raju"; val age = 40; val height = 1.94d
  
  //val (name, age, height) = ("Raju", 40, 1.9d)
  
  val out = s"Hello, $name! Age: ${age + 2}"
  println(out)
  
  
	println(s"Hello, $name! Age: ${age+2}")
  
  println(f"$name%s is $height%2.1f meters tall")
  
  println(raw"a \n b")
  
  printf("We are using %s ver %2.4f", "Scala", 2.12)
  
}