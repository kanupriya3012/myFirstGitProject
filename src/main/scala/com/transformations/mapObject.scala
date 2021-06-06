package com.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object mapObject extends App {

  val spark= SparkSession.builder().appName("map").master("local[*]")
    .getOrCreate()

  val sc= spark.sparkContext
  //logger
  import org.apache.log4j.{Level,Logger}
  Logger.getLogger("org").setLevel(Level.ERROR)

  sc.setLogLevel("error")

  import spark.implicits._
//define schema for prod scenarios
  val csvSchema= StructType(Array(
    StructField("id",IntegerType,true),
    StructField("name",StringType,true),
    StructField("salary",DoubleType,true)
  )
  )

  var csvDF=spark.emptyDataFrame
  csvDF=spark.read.format("csv")
                  .schema(csvSchema)
                  .option("header","true")
                  .load("C:\\xmlTestProject\\dataFiles\\salaryMap.csv")

csvDF.show(false)

  //roadMAp-->id:id+1, name:uppercase, salary: 21% hike using rdd

  println("change df to rdd")
  //map df --> tuple of id, name, salary
  val csvRDD=csvDF.rdd.map(x=> ((x.get(0).toString(),x.get(1).toString(),x.get(2).toString() )))
  //println(csvRDD.getNumPartitions)

  //x._1, x._2 ,x._3 returns 1,2,3 elememts of tuple
  val resultMappedRdd = csvRDD.map(x=>{
    val id= x._1.toInt+1
    val nameFirstLetterUpper= x._2(0).toString.toUpperCase().concat((x._2).substring(1,(x._2).length))
    val nameAllUpper=x._2.toUpperCase()
    val salaryhike=x._3.toDouble+x._3.toDouble*0.21
    (id,nameFirstLetterUpper,nameAllUpper,salaryhike)
  })

  //convert Rdd back to df
  val col=Seq("id","nameFirstLetterUpper","nameAllUpper","salaryHike")
  val finalDF= resultMappedRdd.toDF(col:_*)
  println("showing final result df, only first letter capital")
  finalDF.show()


}
