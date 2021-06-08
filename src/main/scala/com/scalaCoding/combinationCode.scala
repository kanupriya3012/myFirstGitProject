package com.scalaCoding

//not a unique combination code
object combinationCode {

  def main(args:Array[String])={

    val arr=Array(1,2,3,4,5)
    val sum=6

    for (i<-0 to arr.length-1){
      for (j<-0 to arr.length-1){
        if(arr(i)+arr(j)==sum){
          println("{"+arr(i)+","+arr(j)+"}")
        }
      }
    }

  }

}
