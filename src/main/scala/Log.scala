package utils

/**
  * Created by linjiang on 2017/4/24.
  */
object Log {

  var debugEnable = false

  def info(msg: => String): Unit ={
    println("\n\n*****\n"+msg+"\n*****\n\n")
  }

  def debug(msg: => String): Unit ={
    if(debugEnable)
      println("debug:"+msg)
  }

}
