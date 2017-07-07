package com.test

import java.text.SimpleDateFormat
import java.util
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
import lingc.SemanticWordRank

/**
  * Created by xueyuan on 2017/6/26. 根据我们的关键字获取uc的类向量
  */
object clusterText_uc_trainw {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")
  var sc: SparkContext = null
  var hiveContext: HiveContext = null


  //  var article_size = 100000

  //  var save_flag = true


  def main(args: Array[String]): Unit = {
    //args=[threshold_in,threshold,word_size,article_size,partition_num,save_flag,stat_date,stat_date_flag]

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
    var wstat_date = "20170623"
    var vstat_date = "uc170626"
    var training_percent = 0.9
    var topn = 20
    var partition_num = 200
    var err_rate = 0.2
    var table_out = ""
    if (args.length >= 5) {
      wstat_date = args(0)
      vstat_date = args(1)
      training_percent = args(2).toDouble
      topn = args(3).toInt
      partition_num = args(4).toInt
      err_rate = args(5).toDouble
      table_out = args(6)

      println("***********************input wstat_date = " + wstat_date + "*****************************")
      println("***********************input vstat_date = " + vstat_date + "*****************************")
      println("***********************input training_percent = " + training_percent + "*****************************")
      println("***********************input topn = " + topn + "*****************************")
      println("***********************input partition_num = " + partition_num + "*****************************")
      println("***********************input err_rate = " + err_rate + "*****************************")
      println("***********************input table_out = " + table_out + "*****************************")
    }
    //word vec
    val sql_2 = "select word,vec from algo.lj_article_word_vec2 where stat_date='" + vstat_date + "'"
    val word_vec = load_vec(sql_2).collect().toMap
    val word_vec_keySet = word_vec.keySet
    //artical
    val sql_1 = "select a.fid ,a.fcategory from mzreader.ods_t_article_c a where and a.fid=w.id and fresource_type=2  and fid is not null and fcategory is not null and fcontent is not null and fkeywords is not null and fkeywords != ''"
    val id_cate = load_data(sql_1)
    val id_word = SemanticWordRank.process(1, 1, 1).mapPartitions(iter => for (r <- iter) yield {
      (r._1, r._3.map(w => w._1))
    })
    val id_word_br = sc.broadcast(id_word.collect().toMap)

    val id_word_cate = id_cate.mapPartitions(iter => for (r <- iter) yield {
      val map = id_word_br.value
      val id = r._1
      (id, map(id), r._2)
    })
    id_word_cate.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************id_word_cate=" + id_word_cate.count() + "*****************************")
    id_word_cate.unpersist()
    val splits = id_word_cate.randomSplit(Array(training_percent, 1 - training_percent), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    //keywords for each category
    val cate_word_training = training.map(r => (r._3, r._2)).reduceByKey(_ ++ _).map(r => {
      val word_array = r._2
      val word_count = new scala.collection.mutable.HashMap[String, Int]
      val keys = word_array.toSet
      for (k <- keys) {
        word_count += (k -> 0)
      }
      for (w <- word_array) {
        val count = word_count(w)
        word_count.put(w, count + 1)
      }
      val words = word_count.toArray.sortWith(_._2 > _._2).take(topn)

      (r._1, words)
    })
    save_cate_word(cate_word_training)

    //word vec map
    val word_vec_br = sc.broadcast(word_vec)
    //not found word
    val keywords = id_word_cate.flatMap(r => r._2).collect().toSet
    val notfound = (keywords -- word_vec_keySet).toArray
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************keywords=" + keywords.size + "*****************************")
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************notfound=" + notfound.size + "*****************************")
    //get vec for each category
    cate_word_training.repartition(partition_num)
    val cate_vec_training = cate_word_training.mapPartitions(iter => {
      val word_vec_br_value = word_vec_br.value
      val words = word_vec_br_value.keySet
      for (r <- iter) yield {
        val word_array = r._2
        var sum = new Array[Double](word_vec_br_value(word_array(0)._1.toLowerCase).length)
        var word_count = 0.0
        for (i <- 0 until word_array.length) {
          val w = word_array(i)._1.toLowerCase
          val c = word_array(i)._2
          word_count += c
          if (words.contains(w)) {
            val word_vec = word_vec_br_value(w)
            val word_count = new ArrayBuffer[Double]()
            for (w <- word_vec) {
              word_count += (w * c)
            }
            val temp = sum.zip(word_count).map(vv => vv._1 + vv._2)
            sum = temp
          }
        }

        (r._1, sum.map(r => r / word_count))
      }
    })
    cate_vec_training.cache()
    val cate_size = cate_vec_training.count()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************cate_size=" + cate_size + "*****************************")
    //test
    test.repartition(partition_num)
    val cate_vec_test = test.mapPartitions(iter => {
      val word_vec_br_value = word_vec_br.value
      val words = word_vec_br_value.keySet
      for (r <- iter) yield {
        val word_array = r._2
        var sum = new Array[Double](word_vec_br_value(word_array(0).toLowerCase).length)
        for (i <- 0 until word_array.length) {
          val w = word_array(i).toLowerCase
          if (words.contains(w)) {
            val temp = sum.zip(word_vec_br_value(w)).map(vv => vv._1 + vv._2)
            sum = temp
          }
        }
        (r._3, sum)
      }
    })
    //predict
    val cate_vec_training_br = sc.broadcast(cate_vec_training.collect())
    val result = cate_vec_test.mapPartitions(iter => {
      val cate_vec_training_br_value = cate_vec_training_br.value
      for (r <- iter) yield {
        val vec = r._2
        var sim_max = 0.0
        var cate_max = ""
        for ((cate, cate_vec) <- cate_vec_training_br_value) {
          val sim = (cate_vec.zip(vec).map(r => r._1 * r._2).reduce(_ + _)) / (math.sqrt(cate_vec.map(r => math.pow(r, 2)).reduce(_ + _)) * math.sqrt(vec.map(r => math.pow(r, 2)).reduce(_ + _)))
          if (sim_max < sim) {
            cate_max = cate
            sim_max = sim
          }
        }
        (r._1, cate_max)

      }
    })
    result.cache()
    for ((label, pre) <- result.take(100)) {
      println(label + "->" + pre)
    }
    result.repartition(partition_num)
    val cate_result = result.mapPartitions(iter => for (r <- iter) yield {
      if (r._1.equals(r._2)) {
        (r._1, Array(1))
      } else {
        (r._1, Array(0))
      }
    }).reduceByKey(_ ++ _)
    cate_result.cache()
    val cate_error_rate = cate_result.mapPartitions(iter => for (r <- iter) yield {
      val array = r._2
      val size = array.length
      val right = array.sum
      val error_rate = (size - right) / size.toDouble
      (r._1, error_rate)
    }).collect().sortWith(_._2 > _._2)
    val error_rate = 1 - result.filter(r => (r._1).equals(r._2)).count() / result.count().toDouble
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************error_rate=" + error_rate + "*****************************")
    val error_rate2 = 1 - result.filter(r => {
      (r._1.split(",")(0)).equals(r._2.split(",")(0))
    }).count() / result.count().toDouble

    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************error_rate2=" + error_rate2 + "*****************************")
    for ((c, er) <- cate_error_rate) {
      println(c + ":" + er)
    }
    if (error_rate < err_rate) {
      save_data(table_out, cate_vec_training)
    }

  }

  def save_data(table_out: String, cate_vec_training: RDD[(String, Array[Double])]): Unit = {
    val cate_vec = cate_vec_training.map(r => {
      var vec_string = ""
      for (v <- r._2) {
        vec_string += v + ","
      }
      if (vec_string.length > 2) {
        vec_string.substring(0, vec_string.length - 1)
      }

      (r._1, vec_string)

    })
    val candidate_rdd = cate_vec.map(r => Row(r._1, r._2))

    val structType = StructType(
      StructField("cate", StringType, false) ::
        StructField("vec", StringType, false) :: Nil
    )

    //from RDD to DataFrame
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " (cate String, vec String )  stored as textfile"
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val c1 = Calendar.getInstance()
    c1.add(Calendar.DATE, -1)
    val date1 = sdf.format(c1.getTime())
    //    val insertInto_table_sql: String = "insert overwrite table " + table_name + " partition(stat_date = " + date1 + ") select * from "
    val insertInto_table_sql: String = "insert overwrite table " + table_out + "  select * from "
    val temp_table = "xueyuan_temp"
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************save data start*****************************")
    candidate_df.registerTempTable(temp_table)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************register TempTable finished*****************************")
    hiveContext.sql(create_table_sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************create table finished*****************************")
    hiveContext.sql(insertInto_table_sql + temp_table)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************insertInto table finished*****************************")
  }

  def load_data(sql: String): RDD[(Long, String)] = {
    val df = hiveContext.sql(sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************load*****************************")
    val id_cate = df.map(r => (r.getLong(0), r.getString(1))).map(r => {
      var array = new ArrayBuffer[String]()

      (r._1, r._2)
    })
    id_cate

  }

  def load_vec(sql: String): RDD[(String, Array[Double])] = {
    val df = hiveContext.sql(sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************load*****************************")
    val word_vec = df.map(r => (r.getString(0), r.getString(1).split(","))).map(r => {
      val buffer = new ArrayBuffer[Double]()
      for (v <- r._2) {
        buffer += v.toDouble
      }
      (r._1, buffer.toArray)
    })
    word_vec
  }

  def save_words(words: RDD[String]): Unit = {
    val table_out = "algo.xueyuan_uc_notfoundword"
    val temp_table_name2 = "xueyuan_temp_table"
    val candidate_rdd = words.map(r => Row(r))

    val structType = StructType(
      StructField("word", StringType, false) :: Nil
    )

    //from RDD to DataFrame
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " (word String)  stored as textfile"
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

  def save_cate_word(cate_word_training: RDD[(String, Array[(String, Int)])]): Unit = {
    val table_out = "algo.xueyuan_uc_cateword2"
    val temp_table_name2 = "xueyuan_temp_table"
    val candidate_rdd = cate_word_training.map(r => {
      var word = ""
      for ((w, c) <- r._2) {
        word += w + ":" + c + ","
      }
      (r._1, word)
    }).map(r => Row(r._1, r._2))

    val structType = StructType(
      StructField("cate", StringType, false) ::
        StructField("word", StringType, false) :: Nil
    )

    //from RDD to DataFrame
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " (cate String,word String)  stored as textfile"
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


}
