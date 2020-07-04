package com.tekcrux.sparkstreaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf

object TwitterPopularTags {
  def main(args: Array[String]) {
    StreamingExamples.setStreamingLogLevels()

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    val consumerKey = "GOKXQuHLCB1uOs9CcUXEJVt0O"
    val consumerSecret = "B2wsAQWBYv78N9Yj2BMaoHrCnWGLhhWWCGvV7lzWuH7IkjHaSn"
    val accessToken = "39024105-Nd9gHHIJB69BdDBrKwvlyPUjpNoo8REJFArlid9cj"
    val accessTokenSecret = "JLG4cUrVAGNRlJkQEt2LVx15lLrXh7oXOKsbMjYikiI9U"
    
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    
    val stream = TwitterUtils.createStream(ssc, None)
    //val stream = TwitterUtils.createStream(ssc, None, filters)
          
    //Filter hash Tags      
    val hashTags = stream.flatMap(status => status.getText.split(" ")
                         .filter(_.startsWith("#")))
        
    //most repeated hash tags in last 60 milli seconds window
    val topCounts60 = hashTags.map((_, 1))
                        .reduceByKeyAndWindow(_ + _, Seconds(60))
                        .map { case (topic, count) => (count, topic) }    
                        .transform(_.sortByKey(false))     
          
    // Print 10 popular hashtags
    topCounts60.foreachRDD(rdd => {
    val topList = rdd.take(10)

      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}