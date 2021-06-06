package com.sparkProject
import sparkEssentials._

class WordCountClass {

  var textRdd=spark.read.textFile("/FileStore/tables/natureSampleFile.txt").rdd
  var mapRdd= textRdd.flatMap(line=>line.split(" "))
  var keyMapRdd=mapRdd.map(word=>(word,1))

  var reducedRdd=keyMapRdd.reduceByKey(_+_)
  reducedRdd.collect()

}
