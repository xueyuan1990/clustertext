package com.test

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

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
  */
object clusterText_drop {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")

  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val source_table_name: String = "algo.lingcheng_label_docvec_with_blas"
  val temp_table_name2 = "xueyuan_id_wordweight_vector_temp"
  var partition_num = 200
  var stat_date_flag = true

  def main(args: Array[String]): Unit = {
    var article_size = 100000
    //excutor的1-2倍
    var save_flag = true
    var stat_date = 20170320

    var temp_table_name = "algo.xueyuan_id_wordweight_vector_"
    //args=[threshold_in,threshold,word_size,article_size,partition_num,save_flag,stat_date,stat_date_flag]
    var threshold_in = 0.0
    var threshold = 0.0
    var word_size = 0
    var drop = 0
    val userName = "mzsip"
    System.setProperty("user.name", userName)
    System.setProperty("HADOOP_USER_NAME", userName)
    println("***********************start*****************************")
    val sparkConf: SparkConf = new SparkConf().setAppName("xueyuan_clustertext")
    sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    println("***********************sc*****************************")
    sc.hadoopConfiguration.set("mapred.output.compress", "false")
    hiveContext = new HiveContext(sc)
    println("***********************hive*****************************")
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    println("***********************args = " + args.length + "*****************************")

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
      stat_date_flag = args(7).toBoolean
      println("***********************input stat_date_flag = " + stat_date_flag + "*****************************")
      drop = args(8).toInt
      println("***********************input drop = " + drop + "*****************************")
    }

    if (stat_date_flag == false) {
      //only training once
      println("***********************in_out = " + threshold_in + "_" + threshold + "*****************************")
      val table_in = temp_table_name + "_" + word_size + "_" + article_size + "_" + partition_num + "_" + threshold_in.toString.replaceAll("\\.", "") + "_" + stat_date
      val sql = "select * from " + table_in + " where size(split(id,','))>" + drop
      val table_out = temp_table_name + "_" + word_size + "_" + article_size + "_" + partition_num + "_" + threshold.toString.replaceAll("\\.", "") + "_" + stat_date
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************table_in= " + table_in + "*****************************")
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************table_out= " + table_out + "*****************************")
      val articles = load_data(sql, stat_date_flag)
      val clusters = training(threshold, word_size, articles)
      if (save_flag) {
        save_data(table_out, clusters)
      }
    } else {
      //training from threshold_in to threshold
      var threshold_temp = threshold_in;
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************threshold_temp = " + threshold_temp + "*****************************")
      val table_in = source_table_name
      val sql = "select * from " + table_in + " where stat_date=" + stat_date + " order by id limit " + article_size
      val table_out = temp_table_name + "_" + word_size + "_" + article_size + "_" + partition_num + "_" + threshold_temp.toString.replaceAll("\\.", "") + "_" + stat_date
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************table_in= " + table_in + "*****************************")
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************table_out= " + table_out + "*****************************")
      val articles = load_data(sql, stat_date_flag)
      var clusters = training(threshold_temp, word_size, articles)
      if (save_flag) {
        save_data(table_out, clusters)
      }
      threshold_temp = threshold_temp - 0.1
      while (threshold_temp >= threshold) {
        println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************threshold_temp = " + threshold_temp + "*****************************")
        clusters = training(threshold_temp, word_size, clusters)
        if (save_flag) {
          val table_out = temp_table_name + "_" + word_size + "_" + article_size + "_" + partition_num + "_" + threshold_temp.toString.replaceAll("\\.", "") + "_" + stat_date
          save_data(table_out, clusters)
        }
        threshold_temp = threshold_temp - 0.1
      }

    }
  }


  def load_data(sql: String, stat_date_flag: Boolean): RDD[(Array[String], Array[(String, Double)], Array[Double])] = {
    val df = hiveContext.sql(sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************load:" + sql + "*****************************")
    val id_wordweight_vector_rdd_all = df.map(r => (r.getString(0).split(","), r.getString(1).split(","), r.getString(2).split(","))).map(r => {
      val word_weight = r._2
      val vector = r._3
      var word_weigth_map: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]()
      val vector_arraybuffer: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      if (stat_date_flag == true) {
        for (ww <- word_weight) {
          //          println(stat_date_flag + ww)
          val ww_array = ww.split(":")
          word_weigth_map += ((ww_array(0), 1.0))
        }
      } else {
        for (ww <- word_weight) {
          //          println(stat_date_flag + ww)
          val ww_array = ww.split(":")
          word_weigth_map += ((ww_array(0), ww_array(1).toDouble))
        }
      }
      for (v <- vector) {
        vector_arraybuffer += v.toDouble
      }
      val vector_array = vector_arraybuffer.toArray
      (r._1, word_weigth_map.toArray, vector_array)
    })

    id_wordweight_vector_rdd_all

  }

  def save_data(table_out: String, clusters: RDD[(Array[String], Array[(String, Double)], Array[Double])]): Unit = {
    val articleids_wordweight_vector = clusters.map(r => {
      var articleid_string = ""
      for (id <- r._1) {
        articleid_string += id + ","
      }
      if (articleid_string.length > 2) {
        articleid_string.substring(0, articleid_string.length - 1)
      }
      var word_weight_string = ""
      for ((word, weight) <- r._2) {
        word_weight_string += word + ":" + weight + ","
      }
      if (word_weight_string.length > 2) {
        word_weight_string.substring(0, word_weight_string.length - 1)
      }
      var vector_string = ""
      for (vector <- r._3) {
        vector_string += vector + ","
      }
      if (vector_string.length > 2) {
        vector_string.substring(0, vector_string.length - 1)
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
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************save data start*****************************")
    candidate_df.registerTempTable(temp_table_name2)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************register TempTable finished*****************************")
    hiveContext.sql(create_table_sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************create table finished*****************************")
    hiveContext.sql(insertInto_table_sql + temp_table_name2)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************insertInto table finished*****************************")
  }


  def training(threshold: Double, word_size: Int, articles: RDD[(Array[String], Array[(String, Double)], Array[Double])]): RDD[(Array[String], Array[(String, Double)], Array[Double])] = {
    val threshold_br = sc.broadcast(threshold)
    val word_size_br = sc.broadcast(word_size)

    def find_cluster(iterator: Iterator[(Array[String], Array[(String, Double)], Array[Double])]): Iterator[(Array[String], Array[(String, Double)], Array[Double])] = {
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************find_cluster*****************************")
      var id_wordweight_vector: ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double])] = ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double])]()
      var clusters: ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double])] = new ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double])]()
      val threshold = threshold_br.value
      val word_size = word_size_br.value
      while (iterator.hasNext) {
        val next = iterator.next()
        id_wordweight_vector += next
      }
      //cluster the first and second
      if (id_wordweight_vector.size >= 1) {
        val data0 = id_wordweight_vector(0)
        clusters += data0
        //cluster the rest
        for (i <- 1 until id_wordweight_vector.size) {
          val (c, count_num) = find_cluster_rest(clusters, id_wordweight_vector(i), threshold, word_size)
          clusters = c
          //          println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************count_num = " + count_num + " *****************************")
        }
      }

      clusters.iterator
    }


    //start train
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************train start*****************************")
    articles.repartition(partition_num)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************partition_num = " + partition_num + "*****************************")
    var articles_temp = articles
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************stat_date_flag = " + stat_date_flag + "*****************************")
//    if (stat_date_flag == false) {
//      //get vector by weight
//      val articles_num = articles.map(r => {
//        val array = r._1
//        array.length
//      }).cache()
//      for (w <- articles_num.take(10)) {
//        println("articles_num=" + w)
//      }
//      val articles_num_max = articles_num.max().toDouble
//      println("articles_num_max=" + articles_num_max)
//      val weight = articles_num.map(r => r / articles_num_max)
//      for (w <- weight.collect()) {
//        println("w=" + w)
//      }
//      val articles_by_weight = articles.zip(weight).map(r => {
//        val vec = r._1._3
//        var vec_w = new ArrayBuffer[Double]()
//        val w = r._2
//        for (v <- vec) {
//          vec_w += v * w
//          println("v=" + v + ",w=" + w)
//        }
//
//        (r._1._1, r._1._2, vec_w.toArray)
//
//      })
//
//      articles_temp = articles_by_weight
//    }
    var clusters_temp = articles_temp.mapPartitions(find_cluster)
    clusters_temp.cache()
    //convergence
    var size1 = clusters_temp.count()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************clusters.size = " + size1 + "*****************************")
    var temp1 = clusters_temp.mapPartitions(find_cluster)
    temp1.cache()
    var size2 = temp1.count()
    clusters_temp.unpersist()
    clusters_temp = temp1
    clusters_temp.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************clusters.size = " + size2 + "*****************************")
    while (size1 != size2) {
      size1 = size2
      val temp2 = clusters_temp.mapPartitions(find_cluster)
      temp2.cache()
      size2 = temp2.count()
      clusters_temp.unpersist()
      clusters_temp = temp2
      clusters_temp.cache()
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************clusters.size = " + size2 + "*****************************")
    }
    //    var partition_num_temp = partition_num / 2
    //    while (partition_num_temp >= 1) {
    //      clusters_temp.coalesce(partition_num_temp, false)
    //      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************partition_num = " + partition_num_temp + "*****************************")
    //      temp = clusters_temp.mapPartitions(find_cluster)
    //      clusters_temp = temp
    //
    //      //convergence
    //      var size1 = clusters_temp.count()
    //      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************clusters.size = " + size1 + "*****************************")
    //      temp = clusters_temp.mapPartitions(find_cluster)
    //      clusters_temp = temp
    //      var size2 = clusters_temp.count()
    //      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************clusters.size = " + size2 + "*****************************")
    //      while (size1 != size2) {
    //        size1 = size2
    //        temp = clusters_temp.mapPartitions(find_cluster)
    //        clusters_temp = temp
    //
    //        size2 = clusters_temp.count()
    //        println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************clusters.size = " + size2 + "*****************************")
    //      }
    //      partition_num_temp = partition_num_temp / 2
    //    }
    var clusters = clusters_temp.map(r => (r._1, r._2, r._3))
    clusters
  }


  def find_cluster_rest(clusters: ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double])], article: (Array[String], Array[(String, Double)], Array[Double]), threshold: Double, word_size: Int): (ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double])], Int) = {
    var clusters_temp: ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double], Int)] = new ArrayBuffer[(Array[String], Array[(String, Double)], Array[Double], Int)]
    val article_words = article._2.toMap.keySet
    for (c <- clusters) {
      val clusters_words = c._2.toMap.keySet
      val words = clusters_words & article_words
      val cluster = (c._1, c._2, c._3, words.size)
      clusters_temp += cluster
    }
    //sort clusters_temp by words_size
    val clusters_sorted = clusters_temp.zipWithIndex.sortWith(_._1._4 > _._1._4).toArray
    var find = false
    var count_num = 0
    for (c <- clusters_sorted if find == false) {
      count_num += 1
      val cluster_vector_temp = c._1._3
      val size1 = c._1._1.length
      val article_vector_temp = article._3
      val size2 = article._1.length
      val cluster_vector = new ArrayBuffer[Double]()
      val article_vector = new ArrayBuffer[Double]()
      for (i <- cluster_vector_temp) {
        cluster_vector += i / size1
      }
      for (j <- article_vector_temp) {
        article_vector += j / size2
      }
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
          word_weight_union = word_weight_union.take(word_size)
        }
        val cluster = (id_union, word_weight_union, vector_union)
        clusters(index) = cluster
      }
    }
    if (find == false) {
      count_num = -count_num
      clusters += article
    }
    (clusters, count_num)
  }


  //union two word_weight map
  def get_word_weight(word_weigth_1: Array[(String, Double)], word_weigth_2: Array[(String, Double)]): Array[(String, Double)] = {
    val word_weigth_map1 = word_weigth_1.toMap
    val word_weigth_map2 = word_weigth_2.toMap

    val words = word_weigth_map1.keySet ++ word_weigth_map2.keySet
    var word_weight_union: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]()
    for (word <- words) {
      val weight = word_weigth_map1.getOrElse(word, 0.0) + word_weigth_map2.getOrElse(word, 0.0)
      word_weight_union += (word -> weight)
    }
    word_weight_union = word_weight_union.sortWith(_._2 > _._2)
    word_weight_union.toArray
  }
}
