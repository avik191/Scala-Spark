package org.spark

import com.typesafe.config.ConfigFactory

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec

object PopularMovies {

  def getMovieIdAndNames(): Map[Int, String] = {
    
    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("D:\\spark\\ml-100k\\u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

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

    // Create a broadcast variable of our ID -> movie name map
    var nameDict = sparkContext.broadcast(getMovieIdAndNames)
    
    // Load up each line of the ratings data into an RDD
    val lines = sparkContext.textFile("D:\\spark\\ml-100k\\u.data")

    val rdd = lines.map(l => {
      (l.toString().split("\t")(1).toInt, 1)
    })

    val movieCount = rdd.reduceByKey((x, y) => x + y)
    val sortedMovieCount = movieCount.map(data => (data._2, data._1)).sortByKey()

    // for printing movie count along with movie id
    //sortedMovieCount.collect().foreach(println)
    
    val sortedMoviesWithNames = sortedMovieCount.map(data => {
                                                   (nameDict.value(data._2), data._1)
                                              })
                                              
     sortedMoviesWithNames.collect().foreach(println)                                         
  }

}