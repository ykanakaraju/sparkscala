package socket

import org.apache.spark.sql.SparkSession

object SocketConsoleWordCount2 extends App {
  
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
      .format("console")
      .start()

    query.awaitTermination() 
}