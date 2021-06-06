package com.sparkProject

import org.apache.spark.sql.SparkSession

object sparkEssentials extends App{

  val spark= SparkSession.builder().appName("spark sessions").master("local[*]")
    .getOrCreate()

  val sc= spark.sparkContext

}
