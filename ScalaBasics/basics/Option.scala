package com.tekcrux.basics

object Option extends App {
  
  val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
      
  println("capitals.get( \"France\" ) : " +  capitals.get( "France" ))
  println("capitals.get( \"India\" ) : " +  capitals.get( "India" ))
  
  println("show(capitals.get( \"India\")) : " + show(capitals.get( "India")) )
      
  def show(x: Option[String]) = x match {
      case Some(s) => s
      case None => "?"
  }
}

