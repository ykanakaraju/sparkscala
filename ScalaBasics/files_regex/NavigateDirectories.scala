package com.tekcrux.files_regex

import java.nio.file._
import java.io.File

object NavigateDirectories extends App {
  
    val dirname = "."   // ".."
    //val entries = Files.walk(Paths.get(dirname)) // or Files.list
    val directory = new File(dirname)
    
    directory.listFiles().map(_.getName).foreach(println)
    
    /*
    println("*********************************************")
    
    val javaDirName = new java.io.File(".")
    for (d <- subdirs(javaDirName)) println(d)
    
    def subdirs(dir: File): Iterator[File] = {
       val children = dir.listFiles.filter(_.isDirectory)
       children.toIterator ++ children.toIterator.flatMap(subdirs _)
    }
    */
}