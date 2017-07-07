package com.test

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, mutable}

/**
  * Created by xueyuan on 2017/5/12.
  */
object get_sim_ids {
  var article_size = 100000
  var partition_num = 200
  //excutor的1-2倍
  var save_flag = true
  var stat_date = 20170320
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val temp_table_name2 = "xueyuan_id_wordweight_vector_temp_sim"

  def main(args: Array[String]): Unit = {
    //args=[threshold_in,threshold,word_size,article_size,partition_num,save_flag]
    var threshold_in = 0.0
    var threshold = 0.0
    var word_size = 0
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
    //      args=[threshold_in,threshold,word_size,article_size,partition_num,save_flag,stat_data]
    if (args.length >= 8) {
      threshold_in = args(0).toDouble
      println("***********************input threshold_in = " + threshold_in + "*****************************")
      threshold = args(1).toDouble
      println("***********************input threshold = " + threshold + "*****************************")
      word_size = args(2).toInt
      println("***********************input word_size = " + word_size + "*****************************")
      article_size = args(3).toInt
      println("***********************input article_size = " + article_size + "*****************************")
      partition_num = args(4).toInt
      println("***********************input partition_num = " + partition_num + "*****************************")
      save_flag = args(5).toBoolean
      println("***********************input save_flag = " + save_flag + "*****************************")
      stat_date = args(6).toInt
      println("***********************input table_ = " + stat_date + "*****************************")
      val id1 = args(7)
      val (data0, data_all0) = load_data(id1, stat_date)
      val data = data0.collect()
      val data_all = data_all0.collect()
      var result: ArrayBuffer[(Array[String], Map[String, Double], Array[Double])] = new ArrayBuffer[(Array[String], Map[String, Double], Array[Double])]()
      println("***********************load*****************************")
      var v1 = data(0)._3
      for (d <- data_all if result.size < article_size) {
        var v2 = d._3
        var sim_data = sim(v1, v2)
        if (sim_data > threshold_in) {
          result += d
        }
      }


      println("***********************result.size" + result.size + "*****************************")
      //caculate sim of each two article
      //var sim_array = new ArrayBuffer[Double]()
      var sim_array = Array.ofDim[Double](result.size, result.size)
      for (i <- 0 until result.size) {
        for (j <- 0 until result.size) {
          sim_array(i)(j) = sim(result(i)._3, result(j)._3)
          val sim_ = sim_array(i)(j)
          if (sim_ > 0.9) {
            //print(sim_array(i)(j) + ", ")
          } else if (sim_ > 0.8) {
            //print(sim_array(i)(j) + ", ")
          } else {
            print(sim_array(i)(j) + "***<" + result(i)._1(0) + "-" + result(j)._1(0) + ">***, ")
          }

        }
        println()
      }

      //cluster
      val savedata = training(threshold, word_size, sc.parallelize(result))
      if (save_flag) {
        save_data("algo.xueyuan_sim_data" + "_" + threshold_in.toString.replaceAll("\\.", "") + "_" + threshold.toString.replaceAll("\\.", ""), savedata)
      }
    }

  }

  def save_data(table_out: String, clusters: RDD[(Array[String], Map[String, Double], Array[Double])]): Unit = {
    val articleids_wordweight_vector = clusters.map(r => {
      var articleid_string = ""
      for (id <- r._1) {
        articleid_string += id + ","
      }
      var word_weight_string = ""
      for ((word, weight) <- r._2) {
        word_weight_string += word + ":" + weight + ","
      }

      var vector_string = ""
      for (vector <- r._3) {
        vector_string += vector + ","
      }
      (articleid_string, word_weight_string, vector_string)

    })
    val candidate_rdd = articleids_wordweight_vector.map(r => Row(r._1, r._2, r._3))

    val structType = StructType(
      StructField("id", StringType, false) ::
        StructField("words", StringType, false) ::
        StructField("vec", StringType, false) :: Nil
    )

    //from RDD to DataFrame
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " (id String, words String,vec String )  stored as textfile"
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val c1 = Calendar.getInstance()
    c1.add(Calendar.DATE, -1)
    val date1 = sdf.format(c1.getTime())
    //    val insertInto_table_sql: String = "insert overwrite table " + table_name + " partition(stat_date = " + date1 + ") select * from "
    val insertInto_table_sql: String = "insert overwrite table " + table_out + "  select * from "
    println("***********************save data start*****************************")
    candidate_df.registerTempTable(temp_table_name2)
    println("***********************register TempTable finished*****************************")
    hiveContext.sql(create_table_sql)
    println("***********************create table finished*****************************")
    hiveContext.sql(insertInto_table_sql + temp_table_name2)
    println("***********************insertInto table finished*****************************")
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

  def load_data(id1: String, stat_date: Int): (RDD[(String, Map[String, Double], Array[Double])], RDD[(Array[String], Map[String, Double], Array[Double])]) = {
    val sql1 = "select * from algo.lingcheng_label_docvec_with_blas where stat_date=" + stat_date + " and id = '" + id1 + "'"
    val sql2 = "select * from algo.lingcheng_label_docvec_with_blas where stat_date=" + stat_date
    val df1 = hiveContext.sql(sql1)
    val df2 = hiveContext.sql(sql2)
    val id_wordweight_vector_rdd = df1.map(r => (r.getString(0), r.getString(1).split(","), r.getString(2).split(","))).map(r => {
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
    val id_wordweight_vector_rdd_all = df2.map(r => (Array(r.getString(0)), r.getString(1).split(","), r.getString(2).split(","))).map(r => {
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
    (id_wordweight_vector_rdd, id_wordweight_vector_rdd_all)

  }

  def sim(art1: Array[Double], art2: Array[Double]): Double = {
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
    sim
  }

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
