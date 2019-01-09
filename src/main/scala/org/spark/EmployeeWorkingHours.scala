package org.spark

import org.apache.log4j.Logger
import org.apache.log4j._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object EmployeeWorkingHours {
  
  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "D:\\spark\\winutls\\");
    
    val conf = new SparkConf()
        .setAppName("EmplyeeWorkingHours")
        .setMaster("local").
        set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts", "true");
    
    val sparkContext = new SparkContext(conf)
    
    val data = sparkContext.textFile("D:\\spark\\employee_login.csv")
    
    val ob = new EmployeeAnalysis
    ob.getTimeSpentByEachEmp(data)
    //ob.getEmpWithDurationLessThan8(data)
    //ob.getAverageTimeSpentByEmployessOn2(data)
    //ob.getTotalTimeSpentEachDayInDecember(data)
  }
  
}