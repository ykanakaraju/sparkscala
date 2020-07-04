package com.tekcrux.pattern_matching

 abstract class Item
 case class Article(description: String, price: Double) extends Item
 case class Bundle(description: String, discount: Double, items: Item*) extends Item
      

 object CaseClassNested extends App {
            
      val special = Bundle("Father's day special", 
                       20.0, 
                       Article("Spark & Scala", 39.95),
                       Bundle("Spark for in-memory computing", 10.0,
                       		   Article("Something..", 79.95),
                             Article("Something more nested..", 32.95)
                       )
                    )
      
      special match {
        case Bundle(_, _, Article(descr, _), _*) => println( descr )
      }
      
      special match {
        case Bundle(_, _, art @ Article(_, _), rest @ _*) => println(art + ", " + rest)
      }
      
      special match {
        case Bundle(_, _, art @ Article(_, _), rest) => println(art + ", " + rest)
      }
      
      def price(it: Item): Double = it match {
        case Article(_, p) => p
        case Bundle(_, disc, its @ _*) => its.map(price _).sum - disc
      }
      
      price(special)
 }