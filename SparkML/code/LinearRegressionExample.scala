package scala.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression

// House Price Prediction

object LinearRegressionExample extends App {
  
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
    
    val spark = SparkSession
      .builder
      .appName("KMeansExample")
      .master("local[2]")
      .getOrCreate()
     
    // we are taking only one feature: House Size in Sq Ft.  
    val points = spark.createDataFrame(Seq(
                      (1620000, Vectors.dense(2100)),
                      (1690000, Vectors.dense(2300)),
                      (1400000, Vectors.dense(2046)),
                      (2000000, Vectors.dense(4314)),
                      (1060000, Vectors.dense(1244)),
                      (3830000, Vectors.dense(4608)),
                      (1230000, Vectors.dense(2173)),
                      (2400000, Vectors.dense(2750)),
                      (3380000, Vectors.dense(4010)),
                      (1480000, Vectors.dense(1959))
                      )).toDF("label","features")
                      
     val lr = new LinearRegression()
     val model = lr.fit(points)
     
     val test = spark.createDataFrame(Seq(Vectors.dense(2500)).map(Tuple1.apply)).toDF("features")
     test.show()
     
     val results = model.transform(test)
     
     results.printSchema
    
     results.show 
     
     val predictions = results.select("features","prediction").show
}
