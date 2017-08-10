package keyword

import java.text.SimpleDateFormat
import java.util.Date

import com.hankcs.hanlp.HanLP
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.SparkApp

import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/7/28.
  * 找到一个词的扩展词，根据滑动窗口，窗口大小为5
  */
object word_exten {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val windsize = 5
  val partition = 400
  var count = 1500000

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
    if (args.length >= 1) {
      count = args(0).toInt
    }
    val content = load_data()
    content.cache()

    //所有词的集合
    val wordset = content.flatMap(r => r).distinct
    wordset.cache()
    println(sdf_time.format(System.currentTimeMillis()) + "*******************************wordset_size=" + wordset.count() + "****************************")
    //所有词对的统计信息
    val wordpairNum = getWordPairNum(content, wordset).collect()
    //    val word_tf = queryTf(content)
    val word_tf = queryWinTf(wordset, wordpairNum)
    println(sdf_time.format(System.currentTimeMillis()) + "*******************************wordpairNum_size=" + wordpairNum.length + "****************************")
    val wordExtent = getWordExtent(wordset, wordpairNum, word_tf.collect().toMap)
    val res = wordExtent.map(r => Row(r._1, r._2))
    val schema = StructType(List(
      StructField("key", StringType),
      StructField("extenkeys", StringType)
    ))
    SparkApp.saveToHive(res, "algo.xueyuan_key_extent", schema, "20170809")

  }

  def queryWinTf(wordset: RDD[String], wordpairNum: Array[((String, String), Int)]) = {
    val wordpairNum_br = sc.broadcast(wordpairNum)
    val res = wordset.repartition(partition).mapPartitions(iter => for (r <- iter) yield {
      val wordpairNum = wordpairNum_br.value
      val tf = wordpairNum.filter(w=>w._1._1.equals(r)||w._1._2.equals(r)).map(_._2).sum
      (r,tf)
    })
    res
  }

  object Segment {
    private val SEG = HanLP.newSegment()
    SEG.enableNumberQuantifierRecognize(true)
    SEG.enableCustomDictionary(true)
    SEG.enableNameRecognize(false)

    import scala.collection.JavaConversions._

    def stripHtml(content: String): String = {
      val htmlPatten = "<[^>]+?>"
      if (content == null || content.isEmpty()) ""
      else {
        content.replaceAll("\n", " ").replaceAll("<script>.*?</script>", "")
          .replaceAll("(</p>|</br>)\n*", "\n")
          .replaceAll(htmlPatten, " ")
          .replaceAll("(点击加载图片)|(查看原文)|(图片来源)", "")
          .replaceAll("\\s*\n\\s*", "\n")
          .replaceAll("[ \t]+", " ")
          .replaceAll("\n", " ")
      }
    }

    def segment(text: String): Array[String] = {
      if (text == null || text.isEmpty) {
        return Array.empty[String]
      }
      try {
        val t = stripHtml(text).toLowerCase()
        val result = SEG.seg(HanLP.convertToSimplifiedChinese(t))
        //        for (res <- result) {
        //          println(res.word + ":" + res.nature)
        //        }
        result.filter(p => ((p.nature.startsWith("n") || p.nature.startsWith("mz") || p.nature.equals("mbk")) && (p.nature.startsWith("nx") == false))) //(p => p.nature.startsWith("n"))
          .map(p => (p.word /* + ":" + p.nature*/)).toArray
      } catch {
        case e: Exception => Array.empty[String]
      }
    }
  }

  def load_data() = {
    val selectSrc = "select a.fid,a.ftitle,cast(unbase64(a.fcontent) as string) from mzreader.ods_t_article_c a where  a.fid is not null and a.ftitle is not null and a.fcontent is not null and  a.fkeywords is not null order by a.fid limit " + count
    val df = hiveContext.sql(selectSrc)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************load*****************************")
    val res = df.map(r => r.getString(2)).mapPartitions(iter => for (r <- iter) yield {
      var cont_se = new ArrayBuffer[String]()
      for (c <- r.split("</p><p>")) {
        cont_se += Segment.segment(c).mkString(",")
      }
      cont_se.toArray
    }).flatMap(r => r).map(r => r.split(","))
    res
  }

  def getWordExtent(wordset: RDD[String], wordpairNum: Array[((String, String), Int)], word_tf: Map[String, Int]) = {
    val tf_br = sc.broadcast(word_tf)
    val wordpairNum_br = sc.broadcast(wordpairNum)
    val res = wordset.repartition(partition).mapPartitions(iter => for (r <- iter) yield {
      val wordpairNum = wordpairNum_br.value
      val wordlist = wordpairNum.filter(wp => (wp._1._1.equals(r)) || (wp._1._2.equals(r))).map(wp => {
        if (!wp._1._1.equals(r)) {
          (wp._1._1, wp._2)
        } else {
          (wp._1._2, wp._2)
        }
      }).map(w => (w._1, w._2.toDouble / (tf_br.value(w._1) * tf_br.value(r)))).sortWith(_._2 > _._2).take(20).map(wl => wl._1 + ":" + wl._2)
      (r, wordlist.mkString(","))
    })
    res
  }

  def getWordPairNum(content: RDD[(Array[String])], wordset: RDD[String]) = {
    val wordIndexMap = wordset.zipWithUniqueId().collect().toMap
    val wordIndexMap_br = sc.broadcast(wordIndexMap)
    //get wordpair array
    val wordPair = content.repartition(partition).mapPartitions(iter => for (r <- iter) yield {
      val wordIndexMap = wordIndexMap_br.value
      val fillSize = windsize - 2
      val con = Array.fill(fillSize)("") ++ r ++ Array.fill(fillSize)("")
      var wordpair = new ArrayBuffer[(String, String)]()
      for (i <- 0 until con.length - fillSize) {
        var window = new ArrayBuffer[String]()
        for (j <- 0 until windsize if i + j < con.length) {
          if (!"".equals(con(i + j)))
            window += con(i + j)
        }
        val windSize = window.length
        if (windSize > 1) {
          for (m <- 0 until windSize - 1; n <- i + 1 until windSize) {
            val word1 = window(m)
            val word2 = window(n)
            val index1 = wordIndexMap(word1)
            val index2 = wordIndexMap(word2)
            //调整位置
            if (index1 < index2) {
              wordpair += ((word1, word2))
            } else {
              wordpair += ((word2, word1))
            }
          }
        }
      }
      wordpair.toArray
    }).flatMap(r => r)
    val wordpairNum = wordPair.map(r => (r, 1)).reduceByKey(_ + _)
    wordpairNum
  }

  def queryTf(content: RDD[Array[String]]) = {
    content.flatMap(r => r).mapPartitions(iter => for (r <- iter) yield (r, 1)).reduceByKey(_ + _)
  }
}
