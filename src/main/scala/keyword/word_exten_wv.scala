package keyword

import java.text.SimpleDateFormat

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import utils.{Blas, Log, SparkApp}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/8/8.根据词向量计算相似度
  */
object word_exten_wv {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val windsize = 5
  val partition = 400

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
    val t0 = System.currentTimeMillis()
    val word_vec = queryWordVec("20170804")
    val word_vec_keyset = sc.broadcast(word_vec.keySet)
    val t1 = System.currentTimeMillis()
    println("time1:42s" + (t1 - t0))
    val word = loadN().filter(r=>word_vec_keyset.value.contains(r))
    word.cache()
    println(sdf_time.format(System.currentTimeMillis()) + "**************************word_size=" + word.count() + "********************************")
    val t2 = System.currentTimeMillis()
    println("time2:75s" + (t2 - t1))
    word.repartition(partition)
    val wordpair = getWordPair(word.collect())
    println(sdf_time.format(System.currentTimeMillis()) + "**************************wordpair_size=" + wordpair.length + "********************************")
    val t3 = System.currentTimeMillis()
    println("time3:" + (t3 - t2))
    //    val word_abs = getWordAbs(word, word_vec)
    //    println(sdf_time.format(System.currentTimeMillis()) + "**************************word_abs_size=" + word_abs.size + "********************************")
    val t4 = System.currentTimeMillis()
    println("time4:" + (t4 - t3))
    val wordpair_sim = calcSim(wordpair, word_vec).cache()
    println(sdf_time.format(System.currentTimeMillis()) + "**************************wordpair_sim_size=" + wordpair_sim.count() + "********************************")
    val t5 = System.currentTimeMillis()
    println("time5:" + (t5 - t4))
    val wordExtent = getWordExtent(wordpair_sim, word).cache()
    wordpair_sim.unpersist()
    println(sdf_time.format(System.currentTimeMillis()) + "**************************wordExtent_size=" + wordExtent.count() + "********************************")
    word.unpersist()
    val t6 = System.currentTimeMillis()
    println("time6:" + (t6 - t5))
    val res = wordExtent.map(r => Row(r._1, r._2))
    val schema = StructType(List(
      StructField("key", StringType),
      StructField("extenkeys", StringType)
    ))
    SparkApp.saveToHive(res, "algo.xueyuan_key_extent", schema, "20170809")

    wordExtent.unpersist()
  }

  def getWordExtent(wordpair_sim: RDD[((String, String), Double)], word: RDD[String]) = {
    val wordpair_sim_br = sc.broadcast(wordpair_sim.collect())
    val res = word.mapPartitions(iter => for (r <- iter) yield {
      val wordpair_sim = wordpair_sim_br.value
      val simword = wordpair_sim.filter(ws => (ws._1._1.equals(r) || ws._1._2.equals(r))).map(ws => {
        if (ws._1._1.equals(r)) {
          (ws._1._2, ws._2)
        } else {
          (ws._1._1, ws._2)
        }
      }).sortWith(_._2 > _._2).take(10).map(ws => ws._1 + ":" + ws._2)
      (r, simword.mkString(","))
    })
    res
  }

  //  def getWordAbs(word: RDD[String], word_vec: Map[String, Array[Double]]) = {
  //    val word_abs = word.mapPartitions(iter => for (r <- iter) yield {
  //      val vec = word_vec(r)
  //      val abs = math.sqrt(vec.map(math.pow(_, 2)).sum)
  //      (r, abs)
  //    })
  //    word_abs.collect().toMap
  //  }

  def getWordPair(word: Array[String]) = {
    val word_pair = new ArrayBuffer[(String, String)]()
    for (i <- 0 until word.length; j <- 0 until word.length if i < j) {
      word_pair += ((word(i), word(j)))
    }
    word_pair.toArray
  }

  def calcSim(word_pair: Array[(String, String)], word_vec: Map[String, Array[Double]]) = {
    val word_vec_br = sc.broadcast(word_vec)
    //    val word_abs_br = sc.broadcast(word_abs)
    val res = sc.parallelize(word_pair).repartition(partition).mapPartitions(iter => for (r <- iter) yield {
      val word_vec = word_vec_br.value
      //      val word_abs = word_abs_br.value
      val w1 = r._1
      val w2 = r._2
      val v1 = word_vec(w1)
      val v2 = word_vec(w2)
      val sim = v1.zip(v2).map(r => r._1 * r._2).sum / (math.sqrt(v1.map(math.pow(_, 2)).sum) * math.sqrt(v2.map(math.pow(_, 2)).sum))
      ((w1, w2), sim)
    })
    res
  }


  def loadN() = {
    val sql = "select title,content from algo.lc_article_word where stat_date=20170804 and title is not null and content is not null"
    val data = hiveContext.sql(sql).map(r => (r.getString(0).split(",") ++ r.getString(1).split(","))).flatMap(r => r).distinct().map(r => {
      val array = r.split("/")
      if (array.length == 2) {
        (array(0), array(1))
      } else {
        ("", "")
      }
    }).filter(r => (r._2.startsWith("n") || r._2.startsWith("mz") || r._2.equals("mbk")) && (r._2.startsWith("nx") == false))
    data.cache()
    println(sdf_time.format(System.currentTimeMillis()) + "**************************loadn=" + data.count() + "********************************")
    for ((w, n) <- data.take(20)) {
      println(w + ":" + n)
    }
    data.map(_._1).distinct()
  }

  def queryWordVec(stat_date: String) = {
    Log.info("load word vec start")
    val srcSql_2 = s"select word,vec from algo.lj_article_word_vec2 where stat_date='" + stat_date + "'"
    val srcData_2 = SparkApp.hiveContext.sql(srcSql_2)
      .map(r => {
        val vec = r.getString(1).split(",").map(_.toDouble)
        Blas.norm3(vec)
        (r.getString(0), vec)
      })
    srcData_2.collect().toMap
  }
}
