package com.sparkProject
import utils._

object wordCount extends App {

  var textRdd=spark.read.textFile("C:\\xmlTestProject\\dataFiles\\natureSampleFile.txt").rdd
  var mapRdd= textRdd.flatMap(line=>line.split(" "))
  var keyMapRdd=mapRdd.map(word=>(word,1))

  var reducedRdd=keyMapRdd.reduceByKey(_+_)
  reducedRdd.collect()
}
