package com.tekcrux.files_regex

import scala.io._

object BasicFileOps2 extends App {
  
  val filePath: String = "file1.txt"
  val source = Source.fromFile(filePath, "UTF-8")

  var upper = 0
  for (c <- source) if (c.isUpper) upper += 1
  println(f"$upper uppercase")
  
  val source2 = Source.fromFile(filePath, "UTF-8")
  val iter = source2.buffered
  while (iter.hasNext) {
    if (iter.head.isUpper) {     
      while (iter.hasNext && !iter.head.isWhitespace) print(iter.next)
      print(" / ")
    }
    else iter.next
  }
  source.close()
  
  println()
  val source3 = Source.fromFile(filePath, "UTF-8")
  val contents = source3.mkString
  println(f"${contents.substring(0, 10)}...")
}

