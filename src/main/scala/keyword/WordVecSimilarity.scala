package keyword

import com.hankcs.hanlp.utility.LexiconUtility
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import utils.{Blas, Log, SparkApp}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by linjiang on 2017/4/19.
  */
object WordVecSimilarity {


  var srcTable = "algo.lj_article_word_vec2"
  var destTable = "algo.lj_article_word_sim"

  private var simTopN = 25
  private var simThreshold = 0.0
  private var sampleWord = -1
  private var simVectorSize = 200
  var src_stat_date = "1"


  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("usage top-n, sim-threshold sample,stat_date,vecsize,[srctable,destTable]")
      return
    }
    simTopN = args(0).toInt
    simThreshold = args(1).toFloat
    sampleWord = args(2).toInt
    src_stat_date = args(3)
    simVectorSize = args(4).toInt
    if (args.length >= 7) {
      srcTable = args(5)
      destTable = args(6)
    }

    Log.info(
      s"""topn=$simTopN, sim thresh=$simThreshold, sample=$sampleWord, vec size=$simVectorSize
         src_date=$src_stat_date, srcTable=$srcTable, destTable=$destTable""")

    Log.info("\n\n\n********** start cluster *********\n\n\n")
    process(simTopN, simThreshold)
    Log.info("\n\n\n********** end cluster *********\n\n\n")

  }

  def insertIndex(arr: Array[(String, Double)], score: Double): Int = {
    var ind = arr.length - 2
    while (ind >= 0 && arr(ind)._2 < score) {
      arr(ind + 1) = arr(ind)
      ind -= 1
    }
    ind + 1
  }

  def loadN() = {
    val hiveContext = SparkApp.hiveContext
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
//    println(sdf_time.format(System.currentTimeMillis()) + "**************************loadn=" + data.count() + "********************************")
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
        val vec = r.getString(1).split(",").map(_.toFloat)
        Blas.norm2(vec)
        (r.getString(0), vec)
      })
    srcData_2
  }

  def process(topN: Int, threshold: Double): Unit = {
    val word_vec = queryWordVec("20170804")
    println("***********************word_vec*****************************")
    //获取名词
    val word_all = loadN().collect()
    println("***********************word_all*****************************")
    val wordVecs = getWordVec(srcTable, src_stat_date)
    val wordVecs2 = word_vec.filter(word_all.contains(_))
    //.map(r => Row(r._1, r._2.map(w => "%s:%.4f".format(w._1, w._2)).mkString(",")))
    val simWords = calcSimWord(wordVecs, threshold, sampleWord).map(e => {
      Row(e._1, e._2.sortBy(-_._2).take(topN).drop(1).map(w => "%s:%.4f".format(w._1, w._2)).mkString(","))
    }).cache()

    val simcnt = simWords.count()
    Log.info("sim word cnt = " + simcnt)

    val schema = StructType(List(StructField("word", StringType), StructField("simwords", StringType)))

    SparkApp.saveToHive(simWords, destTable, schema, src_stat_date)

    simWords.unpersist()

  }

  def calcSimWord(wordVecs: RDD[(String, Array[Float])],
                  threshold: Double, sampleWord: Int,
                  wordFilter:String =>Boolean = (_:String)=>true)
  : RDD[(String, ArrayBuffer[(String, Double)])] = {

    val wvs = wordVecs.collect().filter(e => wordFilter(e._1))

    Log.info("word cnt = " + wvs.length )
    Thread.sleep(5000)

    val (wordRdd,wordVecBroad) =
      if (sampleWord > 0) {
        Log.info(s"use sample $sampleWord")
        val rdd =SparkApp.sc.parallelize(wvs.take(sampleWord)).repartition(math.min(math.max(10, sampleWord / 20), 100))
        (rdd,SparkApp.sc.broadcast(wvs.take(sampleWord)))
      }
      else (wordVecs.repartition(100),SparkApp.sc.broadcast(wvs))


    wordRdd.mapPartitions(it => {

      val wordVecLocal = wordVecBroad.value
      val ret = it.map(wv => {
        val w1 = wv._1
        val v1 = wv._2

        val wordScore = new ArrayBuffer[(String, Double)]() //Array.fill[(String, Double)](topN + 1)(("_", Double.MinValue))

        var i = 0
        while (i < wordVecLocal.length) {
          val wv2 = wordVecLocal(i)
          val v2 = wv2._2
          val w2 = wv2._1

          val s = Blas.dot(v1, v2)
          if (s > threshold) {
            if(!(s>0.95 && v1==v2))
              wordScore.append((w2, s))
          }
          //          val ind = insertIndex(wordScore, s)
          //          wordScore(ind) = (w2, s)

          i += 1
        }

        (w1, wordScore)
      })
      ret
    })
  }

  def getWordVec(table: String, stat_date: String): RDD[(String, Array[Float])] = {
    val hiveContext = SparkApp.hiveContext
    val df = hiveContext.sql(s"select * from $table  where stat_date='$stat_date'")

    val vsize = simVectorSize
    val wordVecs = df.map(r => {
      val word = r.getString(0)
      val vec = r.getString(1).split(",").map(_.toFloat)
      (word, Blas.norm2(vec))
    }).filter(_._2.length == vsize)
    wordVecs
  }


}
