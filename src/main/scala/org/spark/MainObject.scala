package org.spark
import com.typesafe.config.ConfigFactory

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.log4j._

object MainObject {
  
  def main(args: Array[String]): Unit = {
    
    
      // Set the log level to only print errors
      Logger.getLogger("org").setLevel(Level.ERROR)
    
      System.setProperty("hadoop.home.dir", "D:\\spark\\winutls\\");

      val appConf = ConfigFactory.load()

      val conf = new SparkConf().

      setAppName("").

     
      setMaster("local").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts","true");
      
      val sparkContext = new SparkContext(conf);
      
      // Load up each line of the ratings data into an RDD
      val lines = sparkContext.textFile("D:\\spark\\ml-100k\\u.data")
      
      // Convert each line to a string, split it out by tabs, and extract the third field.
      // (The file format is userID, movieID, rating, timestamp)
      val ratings = lines.map(x => x.toString().split("\t")(2))
      
      // Count up how many times each value (rating) occurs
      val results = ratings.countByValue()
      
      // Sort the resulting map of (rating, count) tuples
      val sortedResults = results.toSeq.sortBy(_._1)
      
      // Print each result on its own line.
      sortedResults.foreach(println)
    
  }
  
  
}