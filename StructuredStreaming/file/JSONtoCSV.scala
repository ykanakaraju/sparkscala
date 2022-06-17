package file

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object JSONtoCSV extends App {
  
    val spark = SparkSession
        .builder.master("local[2]")
        .appName("DataSourceBasic")
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")            
    spark.conf.set("spark.sql.streaming.checkpointLocation", "/home/kanak/scalaspark/checkpoint/csv_sink");
    
    val mySchema = StructType(
                      Array(StructField("id", IntegerType, true),
                          StructField("name", StringType, true),
                          StructField("age", IntegerType, true),
                          StructField("deptid", IntegerType, true)))
    
    val lines = spark.readStream
          .format("json")
          .option("path", "/home/kanak/sparkdir")
          .option("header", "true")
          .option("maxFilesPerTrigger", 1)
          .option("cleanSource", "delete")
          .schema(mySchema)
          .load()     
    
    
    val query = lines
          .writeStream
          .outputMode("append")
          .trigger(Trigger.ProcessingTime("4 seconds"))
          .format("csv")
          .option("header", "true")
          .option("sep", "\t")
          .option("path", "/home/kanak/scalaspark/output/csv")
          .start()

    query.awaitTermination()  
}