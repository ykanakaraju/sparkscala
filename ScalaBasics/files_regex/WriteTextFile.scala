package com.tekcrux.files_regex

import java.io._

object WriteTextFile extends App {
    val out = new PrintWriter("numbers.txt")
    for (i <- 1 to 10) out.print("%d %10.6f\n".format(i, 1.0 / i))
    out.close()
    
    println("Check out the file for output")
}