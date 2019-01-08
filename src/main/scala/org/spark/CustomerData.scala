package org.spark

import com.typesafe.config.ConfigFactory

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.log4j._

object CustomerData {
  
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
      val lines = sparkContext.textFile("D:\\spark\\customer-orders.csv")
      
      val rdd = lines.map(l => {
                         var temp = l.split(",")
                         (temp(0).toInt,temp(2).toFloat)
                      })
      
      val totalSpentByEachCustomer = rdd.reduceByKey((x,y) => x+y)
      
      val sortedRdd = totalSpentByEachCustomer.map(data => (data._2,data._1)).sortByKey()
      
      
      sortedRdd.map(data => (data._2,data._1)).collect().foreach(println)
    
  }
  
}