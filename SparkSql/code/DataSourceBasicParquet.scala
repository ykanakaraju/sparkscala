package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.commons.io.FileUtils;
import java.io.File;

object DataSourceBasicParquet {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
      
    import spark.implicits._
    
    val peopleDF = spark.read.json("people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    FileUtils.deleteQuietly(new File("people.parquet"));
    
    peopleDF.write.parquet("people.parquet")
    //peopleDF.write.json("people2.json")

    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    val parquetFileDF = spark.read.parquet("people.parquet")
    parquetFileDF.printSchema
    
    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    
    spark.stop()
  }
}