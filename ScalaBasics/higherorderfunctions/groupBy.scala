package com.tekcrux.higherorderfunctions

object groupBy extends App {
  
    val names = List("Raju", "Veer", "Harsha", "Hari", "Ramu", "Viswa", "Anand")
    val map = names.groupBy(x => x.substring(0,1))
    map.foreach(println)
}

 