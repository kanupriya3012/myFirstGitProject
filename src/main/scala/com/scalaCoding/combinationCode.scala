package com.scalaCoding

//not a unique combination code , if no combination put no combination
object combinationCode {

  def main(args:Array[String])={

    val arr=Array(1,2,9,1,2)
    val sum=6
    var flag=0
    var flagCom=0

    for (i<-0 to arr.length-1){
      for (j<-0 to arr.length-1){
        if(arr(i)+arr(j)==sum){
          println("{"+arr(i)+","+arr(j)+"}")
          flagCom+=1
        }
        else{ flag+=1 }
      }
    }
  if (flag>0 && flagCom==0){println("no combination")}
  }

}
