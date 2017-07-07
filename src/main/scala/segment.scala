package xueyuan

import com.hankcs.hanlp.HanLP
import utils.{Blas, Log, SparkApp, StrUtil}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD

/**
  * Created by xueyuan
  */


object segment2 {


  def main(args: Array[String]): Unit = {
    val list = HanLP.segment("中国科学院自动化研究所分别做了关于指纹和虹膜识别的学术报告，中国科学院自动化研究所分别做了关于指纹和虹膜识别的学术报告")
    val array = list.toArray().map(r => r.toString).filter(r => r.contains("/n"))
    val set = array.toSet
    val map = new HashMap[String, Int]()
    //    System.out.println(list.size().toString + list);
    System.out.println(array.length);
    for (s <- set) {
      map += ((s, 0))
    }
    for (a <- array) {
      println(a)
    }

  }


}

