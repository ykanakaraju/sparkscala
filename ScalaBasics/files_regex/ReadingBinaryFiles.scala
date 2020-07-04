package com.tekcrux.files_regex

import java.io._

object ReadingBinaryFiles extends App {
    
    // You replace this with any binary format like .zip etc.
    val filePath: String = "E:/Spark/Workspace/ScalaExamples/src/com/tekcrux/files_regex/AirlinesMaxDelay.zip"
  
    val file = new File(filePath)
    val in = new FileInputStream(file)
    val bytes = new Array[Byte](file.length.toInt)
    in.read(bytes)
    in.close()
    
    print(f"Files starts with ${bytes(0)}%c${bytes(1)}%c%n")
}