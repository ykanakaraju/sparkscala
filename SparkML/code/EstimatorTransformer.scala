package scala.examples

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

// predict if a person is a basketball player or not

object EstimatorTransformer extends App {
  
   val spark = SparkSession
          .builder
          .appName("EstimTransf")
          .master("local[2]")
          .getOrCreate()
    
    // training data
    // 0.0 : not a basketball player;  1.0 : is a basketball player      
    val lebron = (1.0, Vectors.dense(80.0,250.0))     
    val tim = (0.0, Vectors.dense(70.0,150.0))     
    val brittany = (1.0, Vectors.dense(80.0,207.0))
    val stacey = (0.0, Vectors.dense(65.0,120.0))
    
    val training = spark.createDataFrame(Seq(lebron,tim,brittany,stacey))
                        .toDF("label","features")
                        
    val estimator = new LogisticRegression
    
    val transformer = estimator.fit(training)
    
    //test data
    val john = Vectors.dense(90.0,270.0)
    val tom = Vectors.dense(62.0,120.0)
    
    val test = spark.createDataFrame(Seq((1.0, john),(0.0, tom)))
                    .toDF("label", "features")
                    
    val results = transformer.transform(test)
    results.printSchema
    
    results.show
    
    val predictions = results.select("features","prediction").show
      
}