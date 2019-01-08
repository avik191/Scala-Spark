package org.spark

import com.typesafe.config.ConfigFactory

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.io.Source

object wordCount {
  
  def main(args: Array[String]): Unit = {
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\spark\\winutls\\");

    val appConf = ConfigFactory.load()

    val conf = new SparkConf().

      setAppName("").

      setMaster("local").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true");

    val sparkContext = new SparkContext(conf);

   // Load up each line of the book data into an RDD
    val rdd = sparkContext.textFile("D:\\spark\\book.txt")
    
    val wordCount = rdd.flatMap(lines => lines.split("\\W+")).map(word => (word,1)).reduceByKey(_ + _)
    //wordCount.collect().foreach(println)
    
    val counts = wordCount.map(word => word._2)
    val avg = counts.reduce(_ + _)/counts.collect().length
    println(avg)
  }
  
}