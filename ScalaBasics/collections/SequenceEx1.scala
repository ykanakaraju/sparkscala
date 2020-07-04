package com.tekcrux.collections

object SequenceEx1 extends App {
  var seq:Seq[Int] = Seq(52,85,1,8,3,2,7)  
  
  seq.foreach((element:Int) => print(element+" "))  
  
  println("\nis Empty: " + seq.isEmpty)  
  println("Ends with (2,7): "+ seq.endsWith(Seq(2,7)))  
  println("contains 8: " + seq.contains(8))  
  println("last index of 3 : " + seq.lastIndexOf(3))  
  println("Reverse order of sequence: " + seq.reverse) 
  
  // Creating Sequences using Range.  
  val seq1 = (1 to 20 by 2)
  seq1.foreach((e:Int) => print(e + " ")) 
  println()
  println("Reverse order of seq1: " + seq1.reverse) 
  
  val seq2 = ('d' until 'h').toSeq
}