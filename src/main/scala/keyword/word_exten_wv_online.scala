package keyword

import java.text.SimpleDateFormat

import org.apache.spark.mllib.linalg.distributed.{MatrixEntry, RowMatrix}
import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import utils.{Blas, Log, SparkApp}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/8/8.根据词向量计算相似度
  */
object word_exten_wv_online {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val windsize = 5
  val partition = 400
  val threshold = 0

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

    //获取wordvec
    val word_vec = queryWordVec("20170804")
    println("***********************word_vec*****************************")
    //获取名词
    val word_all = loadN()
    println("***********************word_all*****************************")
    val t0 = System.currentTimeMillis()
    //过滤word，只保留存在word_vec中的词
    val word_vec_key = sc.broadcast(word_vec.keySet)
    val word = word_all.filter(word_vec_key.value.contains(_)).cache()
    println("***********************word_size=" + word.count() + "*****************************")
    val t1 = System.currentTimeMillis()
    // 归一化
    word.repartition(partition)
    val word_vec_nor = getWordVecNor(word, word_vec).cache()
    println("***********************word_vec_nor_size=" + word_vec_nor.count() + "*****************************")
    word.unpersist()
    val wordExtent = calcSim(word_vec_nor).cache()
    println("***********************wordExtent_size=" + wordExtent.count() + "*****************************")
    val res = wordExtent.map(r => Row(r._1, r._2))
    val schema = StructType(List(
      StructField("key", StringType),
      StructField("extenkeys", StringType)
    ))
    SparkApp.saveToHive(res, "algo.xueyuan_key_extent", schema, "20170810")

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

  def getWordVecNor(word: RDD[String], word_vec: Map[String, Array[Double]]) = {
    val word_vec_br = sc.broadcast(word_vec)
    val word_vec_nor = word.mapPartitions(iter => for (r <- iter) yield {
      val vec = word_vec_br.value(r)
      val abs = math.sqrt(vec.map(math.pow(_, 2)).sum)
      (r, vec.map(_ / abs))
    })
    word_vec_nor
  }


  def calcSim2(word_vec: RDD[(String, Array[Double])]) = {
    val word_vec_br = sc.broadcast(word_vec.collect().toMap)
    val res = word_vec.repartition(partition).mapPartitions(iter => {
      val word_vec_value = word_vec_br.value
      val ret = iter.map(wv => {
        val w1 = wv._1
        val v1 = wv._2
        val wordScore = new ArrayBuffer[(String, Double)]()
        for ((w2, v2) <- word_vec_value if w1.equals(w2) == false) {

          val sim = Blas.dot(v1, v2)
          if (sim >= threshold) {
            wordScore += ((w2, sim))
          }

        }
        (w1, wordScore.toArray.sortWith(_._2 > _._2).take(10).map(ws => ws._1 + ":" + ws._2).mkString(","))
      })
      ret
    })
    res
  }

  def calcSim(word_vec: RDD[(String, Array[Double])]) = {
    val word_vec_br = sc.broadcast(word_vec.collect().toMap)
    val res = word_vec.repartition(partition).mapPartitions(iter => for (wv <- iter) yield {
      val word_vec_value = word_vec_br.value
      //      val ret = iter.map(wv => {
      val w1 = wv._1
      val v1 = wv._2
      val wordScore = new ArrayBuffer[(String, Double)]()
      for ((w2, v2) <- word_vec_value if w1.equals(w2) == false) {
        val sim = Blas.dot(v1, v2)
        if (sim >= threshold) {
          wordScore += ((w2, sim))
        }

      }
      (w1, wordScore.toArray.sortWith(_._2 > _._2).take(10).map(ws => ws._1 + ":" + ws._2).mkString(","))
      //      })
      //      ret
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
