package com.test

import java.text.SimpleDateFormat
import java.util.Calendar

import com.sun.org.apache.xalan.internal.xsltc.compiler.util.IntType
import org.apache.hadoop.mapreduce.Cluster
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._

import scala.collection.{Map, mutable}
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.Path

/**
  * Created by xueyuan on 2017/5/9.
  * 可以手动输入参数的版本，0.9->0.8->0.7 逐层聚类
  */
object clusterText2 {
  var article_size = 100000
  var partition_num = 200
  var save_flag = true
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  var table_name = "algo.xueyuan_cluster_article_wordweight_"
  val tmp_table_name: String = "xueyuan_cluster_article_wordweight_tmp"

  def main(args: Array[String]): Unit = {
    //args=[threshold_min,threshold_inc,threshold,word_size,article_size,partition_num,save_flag]
    var threshold_min = 0.9
    var threshold_inc = 0.1
    var threshold = 0.8
    var word_size = 50
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
    if (args.length > 0) {
      threshold_min = args(0).toDouble
      println("***********************input threshold_min = " + threshold_min + "*****************************")
    }
    if (args.length > 1) {
      threshold_inc = args(1).toDouble
      println("***********************input threshold_inc = " + threshold_inc + "*****************************")
    }
    if (args.length > 2) {
      threshold = args(2).toDouble
      println("***********************input threshold = " + threshold + "*****************************")
    }
    if (args.length > 3) {
      word_size = args(3).toInt
      println("***********************input word_size = " + word_size + "*****************************")
    }
    if (args.length > 4) {
      article_size = args(4).toInt
      println("***********************input article_size = " + article_size + "*****************************")
    }
    if (args.length > 5) {
      partition_num = args(5).toInt
      println("***********************input partition_num = " + partition_num + "*****************************")
    }
    if (args.length > 6) {
      save_flag = args(6).toBoolean
      println("***********************input save_flag = " + save_flag + "*****************************")
    }
    table_name = table_name + threshold.toString.replaceAll("\\.", "") + "_" + threshold_inc.toString.replaceAll("\\.", "") + "_" + word_size + "_" + article_size + "_" + partition_num
    println("***********************table name = " + table_name + "*****************************")
    var articals = load_data()
    var clusters = articals
    var threshold_temp = threshold_min
    while (threshold_temp >= threshold) {
      println("***********************threshold_temp = " + threshold_temp + "*****************************")
      clusters = training(threshold_temp, word_size, clusters)
      threshold_temp = threshold_temp - threshold_inc
    }


    if (save_flag) {
      save_data(clusters)
    }

  }

  def save_data(clusters: RDD[(Array[String], Map[String, Double], Array[Double])]): Unit = {
    val clusterid_articleids_wordweight = clusters.zipWithIndex.map(r => {
      var word_weight_string = ""
      for ((word, weight) <- r._1._2) {
        word_weight_string += word + "->" + weight + "; "
      }
      var articleid_string = ""
      for (id <- r._1._1) {
        articleid_string += id + ", "
      }
      (r._2, articleid_string, word_weight_string)

    })
    val candidate_rdd = clusterid_articleids_wordweight.map(r => Row(r._1.toLong, r._2.toString, r._3.toString))
    //通过编程方式动态构造元数据
    val structType = StructType(
      StructField("clusterid", LongType, false) ::
        StructField("articleids", StringType, false) ::
        StructField("word_weight", StringType, false) :: Nil
    )

    //进行RDD到DataFrame的转换
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    //    val create_table_sql: String = "create table if not exists " + table_name + " (clusterid bigint, articleids String,word_weight String ) partitioned by (stat_date bigint) stored as textfile"
    val create_table_sql: String = "create table if not exists " + table_name + " (clusterid bigint, articleids String,word_weight String )  stored as textfile"
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val c1 = Calendar.getInstance()
    c1.add(Calendar.DATE, -1)
    val date1 = sdf.format(c1.getTime())
    //    val insertInto_table_sql: String = "insert overwrite table " + table_name + " partition(stat_date = " + date1 + ") select * from "
    val insertInto_table_sql: String = "insert overwrite table " + table_name + "  select * from "
    println("***********************save data start*****************************")
    candidate_df.registerTempTable(tmp_table_name)
    println("***********************register TempTable finished*****************************")
    hiveContext.sql(create_table_sql)
    println("***********************create table finished*****************************")
    hiveContext.sql(insertInto_table_sql + tmp_table_name)
    println("***********************insertInto table finished*****************************")
  }

  def load_data(): RDD[(Array[String], Map[String, Double], Array[Double])] = {
    val sql1 = "select * from algo.lingcheng_label_docvec_with_blas"
    val df = hiveContext.sql(sql1)
    println("***********************load*****************************")
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

    val articals = id_wordweight_vector_rdd_all.take(article_size)
    sc.parallelize(articals)
  }

  def training(threshold: Double, word_size: Int, articals: RDD[(Array[String], Map[String, Double], Array[Double])]): RDD[(Array[String], Map[String, Double], Array[Double])] = {
    val id_wordweight_vector_threshold_wordsize_rdd = articals.map(r => (r._1, r._2, r._3, threshold, word_size))
    //start train
    println("***********************train start*****************************")
    id_wordweight_vector_threshold_wordsize_rdd.repartition(partition_num)
    println("***********************partition_num = " + partition_num + "*****************************")
    var clusters_temp = id_wordweight_vector_threshold_wordsize_rdd.mapPartitions(find_cluster)
    println("***********************clusters.size = " + clusters_temp.collect.size + "*****************************")
    var partition_num_temp = partition_num / 2
    while (partition_num_temp >= 1) {
      clusters_temp.coalesce(partition_num_temp, false)
      println("***********************partition_num = " + partition_num_temp + "*****************************")
      clusters_temp = clusters_temp.mapPartitions(find_cluster)

      println("***********************clusters.size = " + clusters_temp.collect.size + "*****************************")
      partition_num_temp = partition_num_temp / 2
    }
    clusters_temp.map(r => (r._1, r._2, r._3))
  }

  //返回cluster
  def find_cluster(iterator: Iterator[(Array[String], Map[String, Double], Array[Double], Double, Int)]): Iterator[(Array[String], Map[String, Double], Array[Double], Double, Int)] = {
    var id_wordweight_vector: ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Double, Int)] = ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Double, Int)]()
    var clusters: ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Double, Int)] = new ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Double, Int)]()
    var threshold = 0.0
    var word_size = 0
    while (iterator.hasNext) {
      val next = iterator.next()
      threshold = next._4
      word_size = next._5
      id_wordweight_vector += next
    }

    //cluster the first and second
    if (id_wordweight_vector.size == 1) {
      val data0 = id_wordweight_vector(0)
      clusters += data0
    } else if (id_wordweight_vector.size >= 2) {
      val data0 = id_wordweight_vector(0)
      val data1 = id_wordweight_vector(1)
      val sim = data0._3.zip(data1._3).map(r => r._1 * r._2).reduce(_ + _) / (math.sqrt(data0._3.map(r => math.pow(r, 2)).reduce(_ + _)) * math.sqrt(data1._3.map(r => math.pow(r, 2)).reduce(_ + _)))
      if (sim > threshold) {
        val id_union = data0._1 ++ data1._1
        var word_weight_union = get_word_weight(data0._2, data1._2)
        if (word_weight_union.size > word_size) {
          word_weight_union = word_weight_union.toList.sortWith(_._2 > _._2).take(word_size).toMap
        }
        val vector_union = data0._3.zip(data1._3).map(r => r._1 + r._2)
        val union = (id_union, word_weight_union, vector_union, threshold, word_size)
        clusters += union
      } else {
        clusters += data0
        clusters += data1
      }

      //cluster the rest
      for (i <- 2 until id_wordweight_vector.size) {
        clusters = find_cluster_rest(clusters, id_wordweight_vector(i), threshold, word_size)
      }
    }

    clusters.iterator
  }


  def find_cluster_rest(clusters: ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Double, Int)], article: (Array[String], Map[String, Double], Array[Double], Double, Int), threshold: Double, word_size: Int): ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Double, Int)] = {
    var clusters_temp: ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Int)] = new ArrayBuffer[(Array[String], Map[String, Double], Array[Double], Int)]
    for (c <- clusters) {
      val clusters_words = c._2.keySet
      val article_words = article._2.keySet
      val words = clusters_words & article_words
      val cluster = (c._1, c._2, c._3, words.size)
      clusters_temp += cluster
    }

    //sort clusters_temp by words_size
    val clusters_sorted = clusters_temp.zipWithIndex.sortWith(_._1._4 > _._1._4).toArray
    var find = false
    for (c <- clusters_sorted if find == false) {
      println("***********************find=false*****************************")
      val cluster_vector = c._1._3
      val article_vector = article._3
      val sim = (cluster_vector.zip(article_vector).map(r => r._1 * r._2).reduce(_ + _)) / (math.sqrt(cluster_vector.map(r => math.pow(r, 2)).reduce(_ + _)) * math.sqrt(article_vector.map(r => math.pow(r, 2)).reduce(_ + _)))
      if (sim > threshold) {
        find = true
        val index = c._2
        val cluster_id = c._1._1.toBuffer
        val article_id = article._1
        val id_union = (cluster_id ++ article_id).toArray
        var word_weight_union = get_word_weight(c._1._2, article._2)
        val vector_union = c._1._3.zip(article._3).map(r => r._1 + r._2)
        if (word_weight_union.size > word_size) {
          word_weight_union = word_weight_union.toList.sortWith(_._2 > _._2).take(word_size).toMap
        }
        val cluster = (id_union, word_weight_union, vector_union, threshold, word_size)
        clusters(index) = cluster
      }
    }
    if (find == false) {
      clusters += article
    }
    clusters
  }


  //union two word_weight map
  def get_word_weight(word_weigth_1: Map[String, Double], word_weigth_2: Map[String, Double]): Map[String, Double] = {
    val words = word_weigth_1.keySet ++ word_weigth_2.keySet
    var word_weigth_union: Map[String, Double] = new mutable.HashMap[String, Double]()
    for (word <- words) {
      val weight = word_weigth_1.getOrElse(word, 0.0) + word_weigth_2.getOrElse(word, 0.0)
      word_weigth_union += (word -> weight)
    }
    word_weigth_union

  }
}
