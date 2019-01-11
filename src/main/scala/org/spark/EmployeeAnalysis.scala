package org.spark

import org.apache.spark.rdd.RDD
import scala.io.Source
import java.util.Collections

class EmployeeAnalysis extends java.io.Serializable{
  
  
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
    //(empId,timeStamp,date)
    val rdd = data.map(line => {
                        val temp = line.split(',')
                        (temp(0),temp(3).toLong,temp(5))
                    })
    
    var empMap : Map[String,List[(String,String)]] = Map()
    //(distinct empId)
    val distinctId = rdd.map(l => l._1).distinct().collect().toList  
    
    //(empId => [(date1,time1),(date2,time2),..])
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
    val empNames = loadEmpIdNames()
    for(emp <- empMap){
      val id = emp._1
      val dateTime = emp._2.toString()
      
      
      val name = empNames(id)
      println(s"${id} \t ${name} \t  ${dateTime}")
    }
    
  }
  
  def getAverageTimeSpentEachDayInDecemeber(data : RDD[String]) = {
    //(empId,timeStamp,date)
    val rdd = data.map(line => {
                        val temp = line.split(',')
                        (temp(0),temp(3).toLong,temp(5))
                    })
    var empMap : Map[String,Double] = Map()
    //(distinct empId)
    val distinctId = rdd.map(l => l._1).distinct().collect().toList 
    
    for(empId <- distinctId){
      val empAvg = rdd.filter(data => data._1 == empId).map(data => (data._3,data._2)).groupByKey().
                                                        mapValues(data => (data.max,data.min)).mapValues(data => {
                                                        val diff = data._1 - data._2
                                                         val seconds =  diff / 1000;                                      
                                                         val hours = seconds / 3600;
                                                         val minutes = (seconds % 3600) / 60;                                  
                                                         (hours+"."+minutes).toDouble
                                                        }).map(data => data._2).reduce(_ + _)/31
                                                        
     empMap += (empId -> empAvg)                                                   
    }
    val sorted = empMap.toSeq.sortWith((x,y) => x._2 > y._2)
    val empNames = loadEmpIdNames()
    for(emp <- sorted){
      val id = emp._1
      val avg = emp._2
     
      val name = empNames(id)
      println(f"${id} \t ${name} \t  ${avg}%.2f hrs")
    }
  }
  
  //Interval (15/12/18 to 20/12/18)
  def getAvgTimeSpentByPTEachDayInInterval(data : RDD[String]) = {
    //(empId,designation,timeStamp,date)
    val rdd = data.map(line => {
                        val temp = line.split(',')
                        (temp(0),temp(2),temp(3).toLong,temp(5))
                    })
   //(distinct empId)
    val distinctId = rdd.filter(data => (data._2 == "PT")).map(l => l._1).distinct().collect().toList 
    
    var empMap : Map[String,Double] = Map()
    for(empId <- distinctId){
      var totalHours = 0.0
      for( i <- 15 to 20){
        val date = i+"/12/2018"
        val empTotal = rdd.map(data => (data._1,data._3,data._4)).filter(data => (data._1 == empId && data._3 == date)).map(data => (data._3,data._2)).
                                              groupByKey().mapValues(data => (data.max,data.min)).mapValues(data => {
                                                        val diff = data._1 - data._2
                                                         val seconds =  diff / 1000;                                      
                                                         val hours = seconds / 3600;
                                                         val minutes = (seconds % 3600) / 60;                                  
                                                         (hours+"."+minutes).toDouble
                                                        }).map(data => data._2).reduce(_ + _)
                                                        
        totalHours += empTotal                                                
      }
      val avg = totalHours/6
      empMap += (empId -> avg)
    }
    
    val sorted = empMap.toSeq.sortWith((x,y) => x._2 > y._2)
    val empNames = loadEmpIdNames()
    for(emp <- sorted){
      val id = emp._1
      val avg = emp._2
     
      val name = empNames(id)
      println(f"${id} \t ${name} \t  ${avg}%.2f hrs")
    }
    
  }
  
  def getAvgTimeEmpInOutInDec(data : RDD[String]) = {
     //(empId,login status,date)
    val rdd = data.map(line => {
                        val temp = line.split(',')
                        (temp(0),temp(4),temp(5))
                    })
    var empMap : Map[String,Int] = Map()
    //(distinct empId)
    val distinctId = rdd.map(l => l._1).distinct().collect().toList
    
    for(empId <- distinctId){
      val empInOutAvg = rdd.filter(data => data._1 == empId).map(data => (data._3,data._2)).
                                                            groupByKey().mapValues(data => data.size)
                                                            .map(data => data._2).reduce(_ + _)/31
                                                            
      empMap += (empId -> empInOutAvg)                                                      
    }
    
    val sorted = empMap.toSeq.sortWith((x,y) => x._2 > y._2)
    val empNames = loadEmpIdNames()
    for(emp <- sorted){
      val id = emp._1
      val avg = emp._2
     
      val name = empNames(id)
      println(f"${id} \t ${name} \t  ${avg}%.2f times")
    }
  }
  
  
  //mapping method for getting time spent in office excluding breaks from the iterable timestamp list of each employee
  def timeSpentInOffice(timestamps : List[Long]) : Long = {
    //val seq = timestamps.toSeq.sortWith((x,y) => x<y)
    val seq = timestamps.sortWith(_ < _)
    var total : Long = 0
    for(i <- 1 until seq.size by 2){
      val diff = seq(i).toLong - seq(i-1).toLong
      total = total + diff
    }
    total
  }
  
  def getTotalTimeSpentExcludingBreaks(data : RDD[String]) = {
    val rdd = data.map(line => {
                      val temp = line.split(',')
                      (temp(0),temp(3).toLong,temp(5))
                  })
                  
    val filtered = rdd.filter(data => data._3 == "2/12/2018").map(data => (data._1,data._2)).groupByKey()
                                                      
     val list = filtered.mapValues(data => data.toList).mapValues(timeSpentInOffice).mapValues(data => {
                                                        val seconds = data/1000
                                                        val hours = seconds / 3600
                                                        val minutes = (seconds % 3600)/60
                                                        (hours+"."+minutes).toDouble
                                                      })
                                                       
   val sorted = list.map(data => (data._2,data._1)).sortByKey(false)
    val empNames = loadEmpIdNames()
    for(emp <- sorted){
      val id = emp._2
      val hour = emp._1
     
      val name = empNames(id)
      println(f"${id} \t ${name} \t  ${hour}%.2f hrs")
    }
    
  }
  
  //mapping method for getting max time spent outside office i.e breaks from the iterable timestamp list of each employee
  def maxTimeSpentOut(timestamps : List[Long]) : Long = {
    //val seq = timestamps.toSeq.sortWith((x,y) => x<y)
    val seq = timestamps.sortWith(_ < _)
    var max : Long = 0
    for(i <- 1 until (seq.size - 1) by 2){
      val diff = seq(i+1).toLong - seq(i).toLong
      if(diff > max) max = diff
    }
    max
  }
  
  
  def getMaxIntervalForWhichEmpWasOut(data : RDD[String]) = {
     val rdd = data.map(line => {
                      val temp = line.split(',')
                      (temp(0),temp(3).toLong,temp(5))
                  })
                  
    val filtered = rdd.filter(data => data._3 == "2/12/2018").map(data => (data._1,data._2)).groupByKey()
                                                      
     val list = filtered.mapValues(data => data.toList).mapValues(maxTimeSpentOut).mapValues(data => {
                                                        val seconds = data/1000
                                                        val hours = seconds / 3600
                                                        val minutes = (seconds % 3600)/60
                                                        (hours+"."+minutes).toDouble
                                                      })
                                                       
   val sorted = list.map(data => (data._2,data._1)).sortByKey(false)
    val empNames = loadEmpIdNames()
    for(emp <- sorted){
      val id = emp._2
      val hour = emp._1
     
      val name = empNames(id)
      println(f"${id} \t ${name} \t  ${hour}%.2f hrs")
    }              
  }
  
}