package socket

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SocketConsoleWordCount4 extends App {
  
  val spark = SparkSession
        .builder.master("local[2]")
        .appName("DataSourceBasic")
        .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")
    
    import spark.implicits._
    
    val host = "localhost"
    val port = 9999
    val windowDuration = "6 seconds"
    val slideDuration = "2 seconds"
        
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .option("includeTimestamp", true)
      .load()
    
    val words = lines.as[(String, Timestamp)].flatMap(line =>
      line._1.split(" ").map(word => (word, line._2))
    ).toDF("word", "timestamp")

    // Group the data by window and word and compute the count of each group
    val windowedCounts = words.groupBy(
                            window($"timestamp", windowDuration, slideDuration), 
                            $"word"
                         )
                        .count()
                        .orderBy("window")

    // Start running the query that prints the windowed word counts to the console
    val query = windowedCounts.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .start()

    query.awaitTermination() 
}