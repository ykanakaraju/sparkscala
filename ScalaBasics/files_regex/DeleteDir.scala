package com.tekcrux.files_regex

import java.io._
import scala.collection.JavaConverters._

object DeleteDir extends App {
  
    try {     
        val systemProperties = System.getProperties.asScala
        val os_name = systemProperties("os.name")     
        //System.getProperty("os.name")
        
        val directoryPath = "testfiles"    
        val workSpacePath = new File(".").getCanonicalPath()    //new File(".").getAbsolutePath()
        var pathToDelete = workSpacePath + "\\" +  directoryPath
        println(pathToDelete)
        
        if (os_name.indexOf("Windows") >= 0) pathToDelete = pathToDelete.replace("\\", "/")       
        println(pathToDelete)
             
        val file = new File( pathToDelete )
        deleteRecursively(file)
    }
    catch{
        case e: Exception => println(e.getMessage())
    }
   
    
    def deleteRecursively(file: File): Unit = {    
      import scala.reflect.io.Directory  
      val directory = new Directory(file)
      directory.deleteRecursively()
    }    
}