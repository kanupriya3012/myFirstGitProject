package com.scalaCoding

object listCodeFindLengthOfElements extends App {

  val listOfElements=List("A","BB","CCC","DDDD","EEEEE")
  val tupleOfElements=(1,2,3,4)
//  println(tupleOfElements._1)

  for (i <-0 to listOfElements.length-1){
    val wordLength=listOfElements(i).length
    println(listOfElements(i)+" - "+wordLength)
  }

}
