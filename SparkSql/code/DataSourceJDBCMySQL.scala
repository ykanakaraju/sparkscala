package tekcrux

import org.apache.spark.sql.SparkSession
import java.util.Properties
import com.mysql.jdbc.Driver
import org.apache.spark.sql.SaveMode

object DataSourceJDBCMySQL {
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceJDBCMySQL")
      .getOrCreate()
      
    import spark.implicits._
    
    
    // Snippet 1: Reading from MySQL using JDBC
    val jdbcDF = spark.read
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/sparkdb")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("dbtable", "emp")
                    .option("user", "root")
                    .option("password", "cloudera")
                    .load()
                    
     jdbcDF.show()
      
     jdbcDF.createOrReplaceTempView("empTempView")
     spark.sql("SELECT * FROM empTempView").show()
     
     
     // Snippet 2:  Writing to MySQL using JDBC
     spark.sql("SELECT * FROM empTempView")
        .write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/sparkdb")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("dbtable", "emp2")
        .option("user", "root")
        .option("password", "cloudera")
        .mode(SaveMode.Overwrite)
        .save()
     
        
    
     // Snippet 3: Writing to MySQL using JDBC
     val ratingsCsvPath = "data/movielens/ratings.csv"
     val ratingsDf = spark.read
                            .format("csv")
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .load(ratingsCsvPath)
     
     ratingsDf.printSchema()
     ratingsDf.show(10)
     
     ratingsDf.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "movielens_ratings2")
      .option("user", "root")
      .option("password", "cloudera")
      .mode(SaveMode.Overwrite)
      .save()
      
      
      spark.close()
      
     
  }
}