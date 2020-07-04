package com.tekcrux.files_regex

import java.io._
import scala.collection.JavaConverters._

object DeleteFiles extends App {
  
  try {     
      val systemProperties = System.getProperties.asScala
      val os_name = systemProperties("os.name")     
      //System.getProperty("os.name")
      
      val directoryPath = "testfiles"    
      val workSpacePath = new File(".").getCanonicalPath()    //new File(".").getAbsolutePath()
      var pathToDelete = workSpacePath + "\\" +  directoryPath
      //println(pathToDelete)      
      if (os_name.indexOf("Windows") >= 0) pathToDelete = pathToDelete.replace("\\", "/")       
      //println(pathToDelete)           
      
      deleteRecursively(new File( pathToDelete ))
  }
  catch{
      case e: Exception => println(e.getMessage())
  }
 
  
  def deleteRecursively(file: File): Unit = {    
     if (file.isDirectory){
       file.listFiles.foreach(deleteRecursively)
     }
     else{       
       if (file.exists && !file.delete){
          throw new Exception(s"Unable to delete ${file.getAbsolutePath}")   
       }
     }
  }  
}