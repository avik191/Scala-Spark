package org.spark

import org.apache.spark.rdd.RDD
import scala.io.Source

class EmployeeAnalysis {
  
  
  def loadEmpIdNames():Map[String,String] = {
  
    var map : Map[String,String] = Map()
    val data = Source.fromFile("D:\\spark\\employee_login.csv").getLines()
    for( l <- data){
        val temp = l.split(',')
        map += (temp(0)->temp(1))       
    }
    
    map
                
  }
  
  def getTimeSpentByEachEmp(data : RDD[String]) = {
    
    //(empId,timeStamp,date)
    val rdd = data.map(l => {
                        val temp = l.split(',')
                        (temp(0),temp(3).toLong,temp(5))
                  })
                  
    //(empId,timeStamp,date = 2/12/2018)
    val filteredRdd = rdd.filter(data => (data._3 == "2/12/2018"))
    //(empId,timeStamp)
    val empIdTimePair = filteredRdd.map(data => (data._1,data._2))
    
    val empIdTimeGroup = empIdTimePair.groupByKey().sortByKey()
    
    //(empId,(max_timestamp,min_timestamp))
    val empIdTimeRange = empIdTimeGroup.mapValues(data => (data.max,data.min))
    //(empId,time duration in office)
    val empIdTime = empIdTimeRange.mapValues(data => {
                                  val diff = data._1 - data._2
                                  val seconds =  diff / 1000;                                      
                                  val hours = seconds / 3600;
                                  val minutes = (seconds % 3600) / 60;                                  
                                  (hours+"."+minutes).toDouble
                            })
    empIdTime.foreach(println)                        
    
  } 
   
  def getEmpWithDurationLessThan8(data : RDD[String])={
     //(empId,timeStamp,date)
    val rdd = data.map(l => {
                        val temp = l.split(',')
                        (temp(0),temp(3).toLong,temp(5))
                  })
                  
    //(empId,timeStamp,date = 2/12/2018)
    val filteredRdd = rdd.filter(data => (data._3 == "2/12/2018"))
    //(empId,timeStamp)
    val empIdTimePair = filteredRdd.map(data => (data._1,data._2))
    
    val empIdTimeGroup = empIdTimePair.groupByKey().sortByKey()
    
    //(empId,(max_timestamp,min_timestamp))
    val empIdTimeRange = empIdTimeGroup.mapValues(data => (data.max,data.min))
    //(empId,time duration in office)
    val empIdTime = empIdTimeRange.mapValues(data => {
                                  val diff = data._1 - data._2
                                  val seconds =  diff / 1000;                                      
                                  val hours = seconds / 3600;
                                  val minutes = (seconds % 3600) / 60;                                  
                                  (hours+"."+minutes).toDouble
                            })
    val filteredList = empIdTime.filter(data => data._2 <= 8)
    val flipped = filteredList.map(l => (l._2,l._1)).sortByKey()
    val sortedList = flipped.map(l => (l._2,l._1))
    val empNames = loadEmpIdNames()
    
    for(result <- sortedList){
      val name = empNames(result._1)
      println(s"${result._1} \t $name \t ${result._2} hrs")
    }
  }
  
  def getAverageTimeSpentByEmployessOn2(data : RDD[String]) = {
        //(empId,timeStamp,date)
    val rdd = data.map(l => {
                        val temp = l.split(',')
                        (temp(0),temp(3).toLong,temp(5))
                  })
                  
    //(empId,timeStamp,date = 2/12/2018)
    val filteredRdd = rdd.filter(data => (data._3 == "2/12/2018"))
    //(empId,timeStamp)
    val empIdTimePair = filteredRdd.map(data => (data._1,data._2))
    
    val empIdTimeGroup = empIdTimePair.groupByKey().sortByKey()
    
    //(empId,(max_timestamp,min_timestamp))
    val empIdTimeRange = empIdTimeGroup.mapValues(data => (data.max,data.min))
    //(empId,time duration in office)
    val empIdTime = empIdTimeRange.mapValues(data => {
                                  val diff = data._1 - data._2
                                  val seconds =  diff / 1000;                                      
                                  val hours = seconds / 3600;
                                  val minutes = (seconds % 3600) / 60;                                  
                                  (hours+"."+minutes).toDouble
                                })
    val durations = empIdTime.map(data => data._2)
    val totalDuaration = durations.reduce((x,y) => x+y)
    val avgDuration = totalDuaration/durations.collect().toList.size
    println(f"Average duartion = ${avgDuration}%.2f hrs")
  }
  
 
  
  def getTotalTimeSpentEachDayInDecember(data : RDD[String]) = {
    
    val rdd = data.map(line => {
                        val temp = line.split(',')
                        (temp(0),temp(3).toLong,temp(5))
                    })
    
    var empMap : Map[String,List[(String,String)]] = Map()
    val distinctId = rdd.map(l => l._1).distinct().collect().toList  
    for(empId <- distinctId){
      val empRdd = rdd.filter(data => data._1 == empId).map(data => (data._3,data._2)).
                                        groupByKey().mapValues(data => (data.max,data.min)).mapValues(data => {
                                                 val diff = data._1 - data._2
                                                 val seconds =  diff / 1000;                                      
                                                 val hours = seconds / 3600;
                                                 val minutes = (seconds % 3600) / 60;                                  
                                                 (hours+"."+minutes+" hrs")
                                               }).sortByKey()
       val list = empRdd.collect().toList
       empMap += (empId -> list)                                        
    }
    
    for(emp <- empMap){
      val id = emp._1
      val dateTime = emp._2.toString()
      
      val empNames = loadEmpIdNames()
      val name = empNames(id)
      println(s"${id} \t ${name} \t  ${dateTime}")
    }
    
  }
  
}