package com.tekcrux.classes

import scala.collection.mutable.ArrayBuffer

class FriendsGroup {
	 class Member(val name: String){
		  val contacts = new ArrayBuffer[Member]
	 }
	
	 private val members = new ArrayBuffer[Member]
	
	 def join(name: String) = {
		  val	m = new Member(name)
		  members += m
		  m
	 }
}


object DemoFriendsGroup extends App {  
  
	  val bdmgroup = new FriendsGroup	  
    val veer = bdmgroup.join("veer")
    val raju = bdmgroup.join("raju")
    val siva = bdmgroup.join("siva")
    val harsha = bdmgroup.join("harsha")
    veer.contacts += raju
    veer.contacts += siva
    veer.contacts += harsha
    
    val mFace = new FriendsGroup
    val barney = mFace.join("Barney")
    
    println("veer's group members")
    for (contacts <- veer.contacts) println(contacts.name)        
}