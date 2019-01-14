package org.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object DataFrame {
  case class EmpData(empno : String, ename : String, designation : String, manager : String, hire_date : String, sal :Int, dept_no : Int, dept : String, machine : String)
  
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    System.setProperty("hadoop.home.dir", "D:\\spark\\winutls\\");


      val conf = new SparkConf().

      setAppName("").

      //setMaster(appConf.getConfig("dev").getString("executionmode")).
      setMaster("local").
      set("spark.executor.memory", "1g").set("spark.driver.allowMultipleContexts","true");
      
      val sparkContext = new SparkContext(conf);
      val sqlContext = new SQLContext(sparkContext)
     // val df = sqlContext.read.format("com.databricks.spark.csv").option("header", true).load("D://spark//emp_data.csv")
      val rdd = sparkContext.textFile("D://spark//emp_data.csv")
      val header = rdd.first()
      val d = rdd.filter(line => line != header).map(line => {
                                                val temp = line.split(',')
                                                EmpData(temp(0),temp(1),temp(2),temp(3),temp(4),temp(5).toInt,temp(6).toInt,temp(7),temp(8))
                                          })
      
                                          
      import org.apache.spark.sql.functions._
      import sqlContext.implicits._
      val df = d.toDF()
      
      println("Printing Schema")
      df.printSchema()
      
      // 1. Average salary of all employees
      df.agg(avg("sal")).collect().foreach(println)
      
      // 2. minimum and max salary of clerk
      df.select("ename","sal").filter("designation = 'CLERK'").agg(max("sal")).show()
      df.select("empno","ename","sal").filter("designation = 'CLERK'").agg(min("sal")).show()
      
      // 3. Display the record of those employees who belongs to IT and has a machine as a Desktop.
      df.select("empno","ename","sal","dept","machine").filter("dept = 'IT' and machine = 'Desktop'").show()
      
      // 4. Display the record of those employee whose name is start with 'A' ends with 'S'
      df.select("empno","ename","sal","dept","machine").filter(df("ename").like("A%S")).show()
      
      // 5. Detail of employees with second max salary
      val row = df.select("sal").sort(desc("sal")).limit(2).collect().toList
      val x = row(1).getInt(0)
      df.select("empno","ename","sal","dept","machine").filter("sal = "+x).show()
      
  }
}