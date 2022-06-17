package file

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object CSVConsole1 extends App {
  
    val spark = SparkSession
        .builder.master("local[2]")
        .appName("DataSourceBasic")
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")            
          
    val mySchema = StructType(
                      Array(StructField("id", IntegerType, true),
                          StructField("name", StringType, true),
                          StructField("age", IntegerType, true),
                          StructField("deptid", IntegerType, true)))
    
    val lines = spark.readStream
          .format("csv")
          .option("path", "/home/kanak/sparkdir")
          .option("header", "true")
          .option("maxFilesPerTrigger", 1)
          .option("cleanSource", "delete")
          .schema(mySchema)
          .load()                  
          
    val counts = lines.groupBy("deptid").count()
    
    val query = counts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()  
}