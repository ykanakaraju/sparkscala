package com.tekcrux.higherorderfunctions

object sortWith extends App {
  
  val str = "Mary had a little lamb"
  str.split(" ").sortWith(_.length < _.length).foreach(println)

}