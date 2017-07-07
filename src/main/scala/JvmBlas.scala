package utils

/**
  * Created by linjiang on 2017/6/7.
  */
object JvmBlas {
  def dot(x:Array[Double],y:Array[Double]):Double = {
    if(x.length != y.length){
      throw new RuntimeException("dot array length not meet x="+x.length+" :y="+y.length)
    }
    var ret = 0.0
    for(i <- 0 until x.length){
      ret += x(i) * y(i)
    }
    ret
  }
  def cos(x:Array[Double],y:Array[Double]):Double = {
    dot(x,y)/(math.sqrt(dot(x,x)) * math.sqrt(dot(y,y)))
  }
  def dot(x:Array[Float],y:Array[Float]):Double = {
    if(x.length != y.length){
      throw new RuntimeException("dot array length not meet x="+x.length+" :y="+y.length)
    }
    var ret = 0.0
    for(i <- 0 until x.length){
      ret += x(i) * y(i)
    }
    ret
  }
  def norm2(x:Array[Float])={
    val n = math.sqrt(dot(x,x)).toFloat
    for(i <- 0 until x.length){
      x(i) /= n
    }
    x
  }
  def cos(x:Array[Float],y:Array[Float]):Double = {
    dot(x,y)/(math.sqrt(dot(x,x)) * math.sqrt(dot(y,y)))
  }
}
