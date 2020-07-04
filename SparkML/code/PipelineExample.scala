package scala.examples

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object PipelineExample extends App {
  
     System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
     
     val spark = SparkSession
      .builder
      .appName("Pipeline")
      .master("local[2]")
      .getOrCreate()
      
      // Prepare training documents from a list of (id, text, label) tuples.
      // Email spam prediction
      val training = spark.createDataFrame(Seq(
        (0L, "spark is a framework for distrinuted computing", 1.0),
        (1L, "hello world", 0.0),
        (2L, "spark is really fast", 1.0),
        (3L, "hadoop mapreduce", 0.0)
      )).toDF("id", "text", "label")
      
      // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
        
      val hashingTF = new HashingTF()
        .setNumFeatures(100)
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
        
      val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.001)
        
      val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
      
      // Fit the pipeline to training documents.
      val model = pipeline.fit(training)
            
      /*   val df1 = tokenizer.transform(training)    => ("id", "text", "label", "words")
       *   val df2 = hashingTF.transform(df1)         => ("id", "text", "label", "words", "features")
       *   val model = lr.fit(df2)
       */
            
      // Now we can optionally save the fitted pipeline to disk
      model.write.overwrite().save("spark-logistic-regression-model")
      
      // We can also save this unfit pipeline to disk
      pipeline.write.overwrite().save("unfit-lr-model")
      
      // And load it back in during production
      //val sameModel = PipelineModel.load("spark-logistic-regression-model")
      
      // Prepare test documents, which are unlabeled (id, text) tuples.
      val test = spark.createDataFrame(Seq(
        (4L, "spark i j k"),
        (5L, "l m n"),
        (6L, "spark hadoop spark"),
        (7L, "apache hadoop")
      )).toDF("id", "text")
      
      // Make predictions on test documents.
      val predictions = model.transform(test)
      
      predictions.printSchema()
      
      predictions.select("id", "text", "probability", "prediction")
        .collect()
        .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
          println(s"($id, $text) --> prob=$prob, prediction=$prediction")
        }
  
}