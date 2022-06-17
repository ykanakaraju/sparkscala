package socket

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object SocketConsoleWordCount3 extends App {
  
  val spark = SparkSession
        .builder.master("local[2]")
        .appName("DataSourceBasic")
        .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    val host = "localhost"
    val port = 9999
        
    val lines = spark
      .readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
    
    val words = lines.as[String].flatMap(_.split(" "))

    val wordCounts = words.groupBy("value").count()
        
    val query = wordCounts.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("4 seconds"))
      .format("console")
      .start()

    query.awaitTermination() 
}