package com

import org.apache.spark.sql.SparkSession

package object ScalaTraining {

  def mainT(args: Array[String]): Unit = {

    case class Event(organizer:String,budget:Int)
    val spark = SparkSession.builder()
      .appName("Create DataFrame from XML File")
      .master("local[*]")
      .getOrCreate()

    val sc=spark.sparkContext

    val eventrdd=sc.textFile("dataFiles\\organizer.csv")
    eventrdd.foreach(println)

  }
}
