package tekcrux

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object DataframesSourcesCSV {
  
    def main(args: Array[String]) {
    
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\");
      
        val spark = SparkSession.builder.master("local[2]").appName("DataSourceCSV").getOrCreate()
      
        spark.sparkContext.setLogLevel("ERROR")
        
        import spark.implicits._
    
        val ratingsCsvPath = "data/movielens/ratings.csv"
        val moviesCsvPath = "data/movielens/movies.csv"
        val movieRatingOutputPath = "data/movielens_out_tsv/"
        
        val ratingsDf = spark.read
                            .format("csv")
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .load(ratingsCsvPath)
                            
        //ratingsDf.take(10).foreach(println)
        ratingsDf.show(5)
        ratingsDf.printSchema
        //ratingsDf.createOrReplaceTempView("ratings")
        //spark.sql("SELECT * FROM ratings WHERE rating < 2 LIMIT 5").show(5)    
                        
        val moviesDf = spark.read
                            .format("csv")
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .load(moviesCsvPath)
        moviesDf.show(5)
        moviesDf.printSchema
        //moviesDf.createOrReplaceTempView("movies")
        //spark.sql("SELECT * FROM movies WHERE movieId < 10").show(5)    
        
        val joinExpr = ratingsDf.col("movieId") === moviesDf.col("movieId")         
        val dfMovieRatings = ratingsDf.join(moviesDf, joinExpr, "inner")        
        dfMovieRatings.show(5)
       
       
        // drop the duplicate column movieId
        // you can not save duplicate columns to csv files
        val dfMovieRatingsClean = dfMovieRatings.drop( moviesDf.col("movieId") )
        dfMovieRatingsClean.show(5, false)
                        
        dfMovieRatingsClean.limit(10)
                      .write
                      .format("csv")                                          
                      .mode(SaveMode.Overwrite)
                      .option("header", true)                       
                      .save(movieRatingOutputPath)
       
                      
        
        spark.stop() 
        
    }

    
}