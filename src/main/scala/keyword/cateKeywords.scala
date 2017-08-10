package keyword

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import utils.SparkApp

import scala.collection.mutable.HashMap

/**
  * Created by xueyuan on 2017/8/1.按照频率提取类代表关键词
  */
object cateKeywords {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")
  var sc: SparkContext = null
  var hiveContext: HiveContext = null

  def main(args: Array[String]): Unit = {
    val userName = "mzsip"
    System.setProperty("user.name", userName)
    System.setProperty("HADOOP_USER_NAME", userName)
    println("***********************start*****************************")
    val sparkConf: SparkConf = new SparkConf().setAppName("xueyuan_clustertext")
    sc = new SparkContext(sparkConf)
    println("***********************sc*****************************")
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    println("***********************hive*****************************")
    val cate_key = loadData()
    val key_num = cate_key.flatMap(_._2).distinct().count()
    println(sdf_time.format(System.currentTimeMillis()) + "**************************key_num=" + key_num +  "***********************************")
    val cate_artnum_keysize_reprekeys = getKeyWordByCate(cate_key)
        val res = cate_artnum_keysize_reprekeys.map(r => Row(r._1, r._2, r._3, r._4.length, r._4.mkString(",")))
        val schema = StructType(List(
          StructField("cate", StringType),
          StructField("artsize", IntegerType),
          StructField("keysize", IntegerType),
          StructField("reprekeysize", IntegerType),
          StructField("reprekeys", StringType)
        ))
        SparkApp.saveToHive(res, "algo.xueyuan_cate_prekey", schema, "20170810")
  }

  def loadData() = {
    val sql = "select cate, kw from algo.xueyuan_tit_p1_con_key_cate where stat_date=20170810 and kw is not null"
    val cate_key = hiveContext.sql(sql).map(r => (r.getString(0), r.getString(1).split(",")))
    cate_key
  }


  def getKeyWordByCate(cate_key: RDD[(String, Array[String])]) = {
    //article num of each cate
    val cate_artnum = cate_key.mapPartitions(iter => for (r <- iter) yield {
      (r._1, 1)
    }).reduceByKey(_ + _).collect.toMap
    val cate_keys = cate_key.reduceByKey(_ ++ _)
    cate_keys.cache()
    //统计信息
    val cate_num = cate_keys.count()
    val key = cate_keys.flatMap(r => r._2).map(r => (r, 1)).reduceByKey(_ + _)
    key.cache()
    val key_num = key.count()
    val key_num100 = key.filter(_._2 > 100).count()
    val key_num500 = key.filter(_._2 > 500).count()
    val key_num1000 = key.filter(_._2 > 1000).count()
    key.unpersist()
    println(sdf_time.format(System.currentTimeMillis()) + "**************************cate_num=" + cate_num + "***********************************")
    println(sdf_time.format(System.currentTimeMillis()) + "**************************key_num=" + key_num + "," + key_num100 + "," + key_num500 + "," + key_num1000 + "***********************************")
    val cate_artnum_br = sc.broadcast(cate_artnum)
    val cate_artnum_keysize_reprekeys = cate_keys.repartition(cate_num.toInt).mapPartitions(iter => for (r <- iter) yield {
      val cate_artnum = cate_artnum_br.value
      val keyarray = r._2
      val keyset = keyarray.toSet
      var key_num = new HashMap[String, Int]()
      for (key <- keyset) {
        key_num.put(key, 0)
      }
      for (key <- keyarray) {
        val num = key_num(key)
        key_num.put(key, num + 1)
      }
      val res = key_num.toArray.sortWith(_._2 > _._2).take(100).map(r => r._1 + ":" + r._2)
      (r._1, cate_artnum(r._1), keyset.size, res)
    })
    cate_artnum_keysize_reprekeys
  }
}
