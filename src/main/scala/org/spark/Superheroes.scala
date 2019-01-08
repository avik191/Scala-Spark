package org.spark

import com.typesafe.config.ConfigFactory

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.io.Source

object Superheroes {
  
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

   // Load up each line of the marvel-graph data into an RDD
    val idNames = sparkContext.textFile("D:\\spark\\marvel-names.txt")
    
    //{Superhero_id,Superhero_name}
    val idNamesMap = idNames.flatMap(line => {
                          
                              val temp = line.split("\"")
                              if(temp.length > 1)
                              {
                                Some(temp(0).trim().toInt,temp(1))
                              }
                              else None
                          })
    
    // Load up each line of the marvel-graph data into an RDD
    val lines = sparkContext.textFile("D:\\spark\\marvel-graph.txt")
    
    //Superhero_id,Apperarance_count
    val idAppearanceMap = lines.map(line => {
                                  val temp = line.split("\\s+")
                                  (temp(0).toInt,temp.length-1)
      
                              })
    val reduced = idAppearanceMap.reduceByKey((x,y) => x+y)
    val flipped = reduced.map(data => (data._2,data._1))
    val maxPair = flipped.max()
    
    
    
    val heroName = idNamesMap.lookup(maxPair._2)(0)
    println(s"$heroName is most popular with ${maxPair._1} connections")
    
    
    
  }
  
}