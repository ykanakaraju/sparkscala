package tekcrux

import org.apache.spark.sql.SparkSession

object DataframeNestedJSON {
  
   def main(args: Array[String]) {
      
      val spark = SparkSession.builder
                  .master("local[2]")
                  .appName("NestedJson")
                  .getOrCreate()
        
      import spark.implicits._
      
      val df = spark.read.json("nestedjson.json")
      df.show()
      df.printSchema()
      
      val df2 = df.withColumn("skill1", $"skills.skill1")
                  .withColumn("skill2", $"skills.skill2")
                  .withColumn("skill3", $"skills.skill3")
                  .drop("skills")
                  
      df2.show()
      df2.printSchema()           
                  
   }
}