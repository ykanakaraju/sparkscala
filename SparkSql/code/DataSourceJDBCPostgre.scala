package tekcrux

import org.apache.spark.sql.SparkSession
import java.util.Properties

object DataSourceJDBCPostgre {
  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DataSourceJDBCPostgre")
      .getOrCreate()
      
    import spark.implicits._
    
    // Reading from JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()    

    // Saving data to a JDBC source
    jdbcDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .save()
      
    // using connection properties   
    val connectionProperties = new Properties()
    connectionProperties.put("user", "username")
    connectionProperties.put("password", "password")
    
    val jdbcDF2 = spark.read
       .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)  

    jdbcDF2.write.jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    
    spark.stop()
  }
}