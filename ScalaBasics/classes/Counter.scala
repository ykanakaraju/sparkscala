package com.tekcrux.classes

object Counter extends App {
    val counter1 = new Counter
    counter1.increment()
    println("counter1 : " + counter1.current())
    println("counter1 currentPrivate: " + counter1.currentPrivate())
    // println(myCounter.current)
    
    val counter2 = new Counter
    counter2.increment()
    counter2.increment()
    println("counter2 : " + counter2.current())
    
    val compare1 = counter1.isLess(counter2)
    println("compare1 : " + compare1)    
}

class Counter {
    private var value = 0             // You must initialize the field
    private[this] var privateValue = 0
    
    // Methods are public by default
    def increment() { value += 1; privateValue += 1 }  
    
    def current() = value
    
    def currentPrivate() = privateValue;
    // def current = value
        
    def isLess(other : Counter) = value < other.value 
    // def isLessPrivate(other : Counter) = privateValue < other.privateValue   // THIS WILL BE AN ERROR
    // If you declare a variable as private[this] it will be accessable only within the class, but not from outside.
}