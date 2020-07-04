package com.tekcrux.files_regex

import java.io._
import scala.collection.mutable.ArrayBuffer

object SerializationTest extends App {
    val kanakaraju = new Person("Kanakaraju")
    val veer = new Person("Veer")
    val harsha = new Person("Harsha")
    kanakaraju.friends += veer
    kanakaraju.friends += harsha
    veer.friends += harsha
    harsha.friends += kanakaraju
    
    //ser
    val out = new ObjectOutputStream(new FileOutputStream("E:/Spark/Workspace/ScalaExamples/src/com/tekcrux/files_regex/test.obj"))
    out.writeObject(kanakaraju)
    out.close()
    
    //deser
    val in = new ObjectInputStream(new FileInputStream("E:/Spark/Workspace/ScalaExamples/src/com/tekcrux/files_regex/test.obj"))
    val savedKanakaraju = in.readObject().asInstanceOf[Person]
    in.close()
    
    savedKanakaraju.description
    savedKanakaraju.friends.map(_.description)
}

class Person(val name: String) extends Serializable {
   val friends = new ArrayBuffer[Person] // ArrayBuffer is serializable
   def description = name + " with friends " + friends.map(_.name).mkString(", ")
}