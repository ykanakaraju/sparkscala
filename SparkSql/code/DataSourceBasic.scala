package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Encoder
import org.apache.commons.io.FileUtils;
import java.io.File;

object DataSourceBasic {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceBasic")
      .getOrCreate()
      
    import spark.implicits._
        
    
    // parquet is the default format that can be read with load method
    val usersDF = spark.read.load("./resources/users.parquet")
    usersDF.show()
    usersDF.printSchema()
    
    FileUtils.deleteQuietly(new File("namesAndFavColors.parquet"));
    usersDF.select("name", "favorite_color").write.parquet("namesAndFavColors.parquet")
    
    // to read from json, use format("json"). 
    // Applies to other formats like parquet, jdbc, orc, csv, text etc
    val peopleDF = spark.read.format("json").load("./resources/people.json")
    peopleDF.select("name", "age").show();
    
    FileUtils.deleteQuietly(new File("namesAndAges.parquet"));
    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
    peopleDF.show()
    
    // Or create a DataFrame directly from sql method
    val sqlDF = spark.sql("SELECT * FROM parquet.`resources/users.parquet`")    
    sqlDF.show()
    
    spark.stop()
  }
}