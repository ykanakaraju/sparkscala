package com.tekcrux.files_regex
import scala.io.Source
import java.io.File

object BasicFileOps extends App {
     
    val filePath: String = "file1.txt"
    //val file_Object = new File(filePath) 
    
    val source = Source.fromFile(filePath, "UTF-8")
    //val source = Source.fromFile(file_Object)
    
      
    val lineIterator = source.getLines  
    for (l <- lineIterator) println(if (l.length <= 25) l else l.substring(0, 22) + "...")  
    source.close()
    
        
    println("1 =====================================================")
    
    // Caution: The sources below aren't closed.
    
    val lines = Source.fromFile(filePath, "UTF-8").getLines.toArray
    for(l <- lines) println(l)
    
    println("2 =====================================================")
    
    val contents = Source.fromFile(filePath, "UTF-8").mkString
    println(contents)
    
    //println("3 =====================================================")
    
    //val tokens = Source.fromFile(filePath, "UTF-8").mkString.split("\\s+")
    //val numbers = for (w <- tokens) yield w.toDouble
    //for(l <- numbers) println(l)
    
    //val numbers = tokens.map(_.toDouble)
    
    
}



