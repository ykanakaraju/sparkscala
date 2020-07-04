package com.tekcrux.collections

object Sets extends App {
  
    val set1 = Set(9, 2, 0, 1) + 10    //Set -> immutable
    print("1 (set1) -> " + set1);  println
    println("set1.hashCode: " + set1.hashCode());  
    println("set1.head: " + set1.head);  
    println("set1.tail: " + set1.tail);  
    println("set1.min: " + set1.min);  
    println("set1.max: " + set1.max);  
    
    println
    
    val set2 = set1 + (4, 5, 8, 9)   
    print("(set2) -> " + set2);  println
    
    for (i <- set2) print(i + " ")
    println
    
    val weekdays = scala.collection.mutable.LinkedHashSet("Mo", "Tu", "We", "Th", "Fr")
    println("LinkedHashSet  >> " + weekdays) 
    
    val sortset = collection.immutable.SortedSet(1, 2, 3, 4, 5, 6)    
    println("sortset  >> " + sortset) 
    
    val digits = Set(1, 7, 2, 9)
    val primes = Set(2, 3, 5, 7)
    
    println("digits  >> " + digits) 
    println("primes  >> " + primes) 
    println("digits contains 0 >> " + digits contains 0) // false
    
    Set(1, 2) subsetOf digits // true  
    
    print("Set(1, 2) subsetOf digits >> ")
    println(Set(1, 2).subsetOf(digits))    
    
    //union
    print("digits union primes >> ")
    println(digits union primes)
    println(digits ++ primes)    // same as above
    
    //intersection
    print("digits & primes >> ")
    println(digits & primes)
    
    //subtract
    print("digits -- primes >> ")
    println(digits -- primes)    
}