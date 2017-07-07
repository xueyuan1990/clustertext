package com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}

/**
  * Created by xueyuan on 2017/5/12.
  */
object get_sim {
  var sc: SparkContext = null
  var hiveContext: HiveContext = null

  def main(args: Array[String]): Unit = {
    val userName = "mzsip"
    System.setProperty("user.name", userName)
    System.setProperty("HADOOP_USER_NAME", userName)
    println("***********************start*****************************")
    val sparkConf: SparkConf = new SparkConf().setAppName("platformTest")
    sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    println("***********************sc*****************************")
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    println("***********************hive*****************************")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    if (args.length >= 3) {
      var id1 = args(0)
      var id2 = args(1)
      var stat_data = args(2).toInt
      val data = load_data(id1, id2, stat_data).collect()
      println("***********************load*****************************")
      var v1 = data(0)._3
      var v2 = data(1)._3
      var sim_data = sim(v1, v2)
    }
  }

  def load_data(id1: String, id2: String, stat_date: Int): RDD[(Array[String], Map[String, Double], Array[Double])] = {
    val sql1 = "select * from algo.lingcheng_label_docvec_with_blas where (stat_date=" + stat_date + " and id = '" + id1 + "') or (id ='" + id2 + "' and stat_date=" + stat_date + ")"
    val df = hiveContext.sql(sql1)
    val id_wordweight_vector_rdd_all = df.map(r => (Array(r.getString(0)), r.getString(1).split(","), r.getString(2).split(","))).map(r => {
      val word_weight = r._2
      val vector = r._3
      var word_weigth_map: Map[String, Double] = new mutable.HashMap[String, Double]()
      val vector_arraybuffer: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      for (ww <- word_weight) {
        val ww_array = ww.split(":")
        word_weigth_map += (ww_array(0) -> ww_array(1).toDouble)
      }
      for (v <- vector) {
        vector_arraybuffer += v.toDouble
      }
      val vector_array = vector_arraybuffer.toArray
      (r._1, word_weigth_map, vector_array)
    })
    id_wordweight_vector_rdd_all

  }

  def sim(art1: Array[Double], art2: Array[Double]): Unit = {
    val member = art1.zip(art2).map(d => d._1 * d._2).reduce(_ + _)
    //求出分母第一个变量值
    val temp1 = math.sqrt(art1.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母第二个变量值
    val temp2 = math.sqrt(art2.map(num => {
      math.pow(num, 2)
    }).reduce(_ + _))
    //求出分母
    val denominator = temp1 * temp2
    val sim = member / denominator
    println("***********************sim=" + sim + "*****************************")
  }
}
