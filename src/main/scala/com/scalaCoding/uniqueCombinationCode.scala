package com.scalaCoding

object uniqueCombinationCode {
//print uniqueCombination
  def main(args: Array[String])={
    val arr=Array(1,2,3,6,5,7)
    val num=8
    val arrLength=arr.length

    for(i<-0 to arrLength-1){
      for(j<-i+1 to arrLength-1){
        if(arr(i)+arr(j)==num){
          println("{"+ arr(i) +"," +arr(j)+"}")
        }
      }
    }
  }
}
