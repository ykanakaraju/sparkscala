package tekcrux

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object DatasetCreation {
  
  case class Person(name: String, age: Long)
    
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder.master("local[2]")
      .appName("DatasetCreation")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("ERROR")  
      
    import spark.implicits._

    val caseClassDS = Seq(Person("Raju", 43), Person("Ram", 33), Person("Rahim", 23)).toDS()
    caseClassDS.printSchema()
    caseClassDS.show()    
    caseClassDS.select("name").show()
    
    caseClassDS.createOrReplaceTempView("person")
    spark.sql("SELECT * FROM person").show()
      
    println("========================================")
    
    val primitiveDS = Seq(1, 2, 3).toDS()
    primitiveDS.printSchema()
    primitiveDS.map(x => x + 1).collect().foreach(println)

    println("========================================")
    
    // DataFrames can be converted to a Dataset by providing a class. 
    // Mapping will be done by name
    val path = "people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
    
    val dfPeople = spark.read.json(path)
    println("dfPeople.getClass : " + dfPeople.getClass)
      
    val df1 = caseClassDS.toDF()
    println(df1.getClass)
    df1.printSchema()
    df1.show()
    
    
    spark.stop()
  }
}