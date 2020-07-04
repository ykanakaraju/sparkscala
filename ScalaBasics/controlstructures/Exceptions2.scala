package com.tekcrux.controlstructures

import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException

object Exceptions2 extends App {   
    try {
       val f = new FileReader("file1.txt")
    } 
    catch {
       case ex: FileNotFoundException => {
          println("Missing file exception")
       }         
       case ex: IOException => {
          println("IO Exception")
       }
    } 
    finally {
       println("Exiting finally...")       
    }   
}