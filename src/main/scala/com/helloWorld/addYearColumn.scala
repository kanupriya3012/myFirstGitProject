package com.helloWorld
import com.sparkProject.utils.spark
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object addYearColumn extends App {
//NULL POINTER EXCEPTIOn
  val schema= StructType(Array(StructField("id",IntegerType,true),
    StructField("name",StringType,true),
    StructField("date",DateType,true)
  ))
  val data=Seq((1, "a","23/03/2020"),(2, "b", "23/04/2020"))
  val col=Seq("id","name","date")
  import spark.implicits._
  val df= data.toDF(col:_*).schema("schema")
}
