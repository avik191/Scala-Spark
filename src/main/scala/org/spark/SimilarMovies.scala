package org.spark

import com.typesafe.config.ConfigFactory

import org.apache.spark.SparkConf

import org.apache.spark.SparkContext
import org.apache.log4j._
import scala.io.Source
import scala.io.Codec
import java.nio.charset.CodingErrorAction
import scala.math.sqrt



object SimilarMovies {
  
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
  
  type movieRatings = (Int,Double)
  type userMovieRatings = (Int,(movieRatings,movieRatings))
  
  def filterMovies(userMovieRating : userMovieRatings) : Boolean = {
    
    val movierating1 = userMovieRating._2._1
    val movieRating2 = userMovieRating._2._2
    
    val movieId1 = movierating1._1
    val movieId2 = movieRating2._1
    
    return movieId1 < movieId2
    
  }
  
  def makePair(userMovieRating : userMovieRatings) = {
    
    val movierating1 = userMovieRating._2._1
    val movieRating2 = userMovieRating._2._2
    
    val movieId1 = movierating1._1
    val rating1 = movierating1._2
    val movieId2 = movieRating2._1
    val rating2 = movieRating2._2
    
    ((movieId1,movieId2),(rating1,rating2))
  }
  
  type RatingPair = (Double, Double)
  type RatingPairs = Iterable[RatingPair]
  
  def computeCosineSimilarity(ratingPairs:RatingPairs): (Double, Int) = {
    var numPairs:Int = 0
    var sum_xx:Double = 0.0
    var sum_yy:Double = 0.0
    var sum_xy:Double = 0.0
    
    for (pair <- ratingPairs) {
      val ratingX = pair._1
      val ratingY = pair._2
      
      sum_xx += ratingX * ratingX
      sum_yy += ratingY * ratingY
      sum_xy += ratingX * ratingY
      numPairs += 1
    }
    
    val numerator:Double = sum_xy
    val denominator = sqrt(sum_xx) * sqrt(sum_yy)
    
    var score:Double = 0.0
    if (denominator != 0) {
      score = numerator / denominator
    }
    
    return (score, numPairs)
  }
  
  def main(args: Array[String]): Unit = {
  
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    System.setProperty("hadoop.home.dir", "D:\\spark\\winutls\\");

    //val appConf = ConfigFactory.load()

    val conf = new SparkConf().

      setAppName("").

      setMaster("local").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true");

    val sparkContext = new SparkContext(conf);

   
    val data = sparkContext.textFile("D:\\spark\\ml-100k\\u.data")
    val movieNames = getMovieIdAndNames()
    
    // make rdd userId,(movieId,rating)
    val ratings = data.map(lines => {
                              val temp = lines.split("\t")
                              (temp(0).toInt,(temp(1).toInt,temp(2).toDouble))
                          })
                          
    //self join to find every pair of movies watched by same user
    // userId,((movieId,rating),(movieId,rating))
    val joinedRatings = ratings.join(ratings)
    
    val filteredJoinedRatings = joinedRatings.filter(filterMovies)
    
    // ((movie1,movie2),(rating1,rating2))
    val moviePair = filteredJoinedRatings.map(makePair)
    
    //(movie1, movie2) = > (rating1, rating2), (rating1, rating2) ...
    val moviePairRatings = moviePair.groupByKey()
    
    val moviePairSimilarities = moviePairRatings.mapValues(computeCosineSimilarity).cache()
    
    //Save the results if desired
    //val sorted = moviePairSimilarities.sortByKey()
    //sorted.saveAsTextFile("D:\\spark\\movie-sims")
    
    val scoreThreshold = 0.97
    val coOccurenceThreshold = 50.0
    val refMovieId = 50 // for movie = "Star Wars"
    
    val filteredMoviePair = moviePairSimilarities.filter(l => {
                                val movieId1 = l._1._1
                                val movieId2 = l._1._2
                                val sim = l._2._1
                                val coOccurance = l._2._2
                                (movieId1 == refMovieId || movieId2 == refMovieId) && sim > scoreThreshold && coOccurance > coOccurenceThreshold
                              })
                              
   val flipped = filteredMoviePair.map(data => (data._2,data._1))
   val results = flipped.sortByKey(false).take(10)
   
   for(result <- results)
   {
     val sim = result._1
     val pair = result._2
     
     val score = sim._1
     val coOccur = sim._2
     val movie1 = pair._1
     val movie2 = pair._2
     var similarMovie = movie1
     
     if(similarMovie == refMovieId) similarMovie = movie2
     
     println(s"Name = ${movieNames(similarMovie)} \t , score = ${score} \t , strength = ${coOccur}")
   }
    
    
    
  }
  
}
