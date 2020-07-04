package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataframeNestedJSON2 {
  
   def main(args: Array[String]) {
            
      val spark = SparkSession.builder.master("local[2]").appName("NestedJson").getOrCreate()
        
      import spark.implicits._
      
      val df = spark.read.json("nestedjson2.json")
      df.show
      df.printSchema
            
      val dfLogin = df.select(explode(df("login"))).toDF("login")
      dfLogin.show
            
      val dfContacts = df.select(explode(df("contacts"))).toDF("contacts")
      dfContacts.printSchema
      dfContacts.select("contacts.phone", "contacts.email").show
            
      val df2 = df.withColumn("phone", $"contacts.phone")
                  .withColumn("email", $"contacts.email")
                  .drop("contacts")
                  
      df2.show
      df2.printSchema         
                  
   }
}