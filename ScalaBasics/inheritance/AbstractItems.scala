package com.tekcrux.inheritance

import collection.immutable.List

object AbstractItems extends App {
  
    // get a List of SimpleItems
    var l: List[Item] = List(new SimpleItem("iMac 21", 1500),	new SimpleItem("iPhone 4s", 800))
    
    val b = new Bundle
    b.add(new SimpleItem("iPad2", 500))
    b.add(new SimpleItem("MacBook Air 13", 1200))
    
    l = b :: l
    
    l.foreach(println(_))
}

abstract class Item {
  	def description: String   // abstract method
  	def price: Double         // abstract method
  	override def toString =  "%s(%s: %f)".format(this.getClass.getSimpleName, description, price)
}

// val can override defs in the abstract super class
class SimpleItem(	override val description: String,	override val price: Double ) extends Item

// This class is used to create a bundle of Items
class Bundle extends Item {
  	private var items: List[Item] = List()
  	
  	// :: operator appends items to a List
  	def add(item : Item) = { items = item :: items }
  	
  	// overriding abstract method
  	def price: Double = items.map(_.price).sum 
  	
  	// overriding abstract method
  	def description: String = items.map(_.description).mkString(", ")
}
