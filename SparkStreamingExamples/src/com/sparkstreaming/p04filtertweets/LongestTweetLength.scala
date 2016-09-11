package com.sparkstreaming.p04filtertweets

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import java.util.concurrent._
import java.util.concurrent.atomic._
import com.sparkstreaming.utils.Utilities._

object LongestTweetLength {
   /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "AverageTweetLength" that runs locally using
    // all CPU cores and one-second batches of data
    val ssc = new StreamingContext("local[*]", "AverageTweetLength", Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)
    
    // Now extract the text of each status update into DStreams using map()
    val statuses = tweets.map(status => status.getText())
    
    // Map this to tweet character lengths.
    val lengths = statuses.map(status => status.length())
    
    lengths.print()
    
    var maxLength = new AtomicInteger(0)

    
    lengths.foreachRDD((rdd, time) => {
      
      var max = rdd.max()
      var count = rdd.count()
      if (count > 0) {
        if (max > maxLength.get()) {
           maxLength.set(max)
      } 
      }
      
      println(maxLength)
    })
    
    // Set a checkpoint directory, and kick it all off
    // I could watch this all day!
    ssc.checkpoint("C:/checkpoint/")
    ssc.start()
    ssc.awaitTermination()
  }  
}