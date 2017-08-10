package xueyuan

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.github.fommil.netlib.F2jBLAS
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import lingc.ExpTable
import utils.{Blas, Log, SparkApp, StrUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/7/4.
  */
object kw_online2 {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val htmlPatter = "<[^>]+?>"
  val WORD_SEP = ","
  val LINE_SEP = WORD_SEP + "###" + WORD_SEP
  var count = 100

  def main(args: Array[String]): Unit = {
    var fid = args(0)
    val srcTable = "mzreader.ods_t_article_c"
    val sqlContext = SparkApp.hiveContext
    val selectSrc = s"select a.fid,a.ftitle,cast(unbase64(a.fcontent) as string) from $srcTable a, algo.xueyuan_article_key b where b.stat_date='20170703' and a.fid=b.id and a.fresource_type=2  and a.fid is not null and a.fkeywords is not null and a.fkeywords != '' and fid=" + fid + " limit 1 "
    val srcData = sqlContext.sql(selectSrc)

    val data = srcData.map(r => {
      (r.getString(1), r.getString(2))
    }).map(r => (r._1, r._2)).take(1)
    val (title, content) = data(0)
    var w1 = 4
    var w2 = 2
    var w3 = 4

    val start = System.currentTimeMillis()
    val kw = process(title, content, w1, w2, w3)
    println(kw.mkString(","))
    val end = System.currentTimeMillis()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************content= " + content + "*****************************")
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************title= " + title + "*****************************")
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************time= " + (end - start) + "*****************************")

  }

  def process(title: String, content: String, w1: Double, w2: Double, w3: Double): Array[String] = {

    val sc = SparkApp.sc
    val time0 = System.currentTimeMillis()
    val wordVecg = queryWordVec()
    val time1 = System.currentTimeMillis()
    val wordIdfBroad = sc.broadcast(queryIdf())
    val trainWords = wordVecg.map(_._1).toSet
    val time2 = System.currentTimeMillis()
    val se = segmentArticle(title.toLowerCase(), content.toLowerCase())
    val time3 = System.currentTimeMillis()
    println(se._1)
    println(se._2)
    val title_se = getWords(se._1).filter(trainWords.isEmpty || trainWords.contains(_))
    val content_se =
      if (StrUtil.isEmpty(se._2)) {
        Array.empty[String]
      }
      else {
        (getWords(se._2).filter(trainWords.isEmpty || trainWords.contains(_)))
      }

    val expTableBroad = sc.broadcast(new ExpTable)
    val allWords_dup = title_se ++ content_se
    val allWords = allWords_dup.distinct
    val vec = allWords.map { r =>
      val vecOption = wordVecg.get(r)
      val vec_in = vecOption match {
        case Some(s) => s
        case None => null
      }
      (r, vec_in)
    }.filter(_._2 != null)

    val wordsim = vec.map { v =>
      val sim = vec.filter(_._1 != v._1).map(r => Blas.cos(r._2, v._2)).sum
      (v._1, sim)
    }


    val word_sim_tfidf_tit = wordsim.map(r => {
      val w_tfidf = wordIdfBroad.value
      val w_set = w_tfidf.keySet
      val w = r._1
      val sim = r._2
      var tfidf = 0.0
      if (w_set.contains(w)) {
        tfidf = w_tfidf(w)
      }
      var tit = 0.0
      if (title.contains(w)) {
        tit = 1
      }
      (w, sim, tfidf, tit)
    })

    //归一化
    var sim_max = 0.0
    var sim_min = 0.0
    var tfidf_max = 0.0
    var tfidf_min = 0.0
    for ((w, sim, tfidf, tit) <- word_sim_tfidf_tit) {
      if (sim_max < sim) {
        sim_max = sim
      } else {
        sim_min = sim
      }
      if (tfidf_max < tfidf) {
        tfidf_max = tfidf
      } else {
        tfidf_min = tfidf
      }
    }
    var sim_div = sim_max - sim_min
    var tfidf_div = tfidf_max - tfidf_min
    if (sim_div == 0) {
      sim_div = 1
    }
    if (tfidf_div == 0) {
      tfidf_div = 1
    }
    var word_score = new ArrayBuffer[(String, Double)]()
    for ((w, sim, tfidf, tit) <- word_sim_tfidf_tit) {
      word_score += ((w, w1 * (sim - sim_min) / sim_div + w2 * (tfidf - tfidf_min) / tfidf_div + w3 * tit))
    }

    val kw = new ArrayBuffer[String]()
    for ((w, s) <- word_score.sortWith(_._2 > _._2).take(3)) {
      kw += w
    }
    val time4 = System.currentTimeMillis()
    println((time1 - time0) + "," + (time2 - time1) + "," + (time3 - time2) + "," + (time4 - time3))
    kw.toArray
  }


  def getWords(content: String) = {
    if (StrUtil.isEmpty(content)) Array.empty[String]
    else {
      val word = content.split(',').map(t => {
        val end = t.lastIndexOf("/")
        if (end > 0) {
          val w = t.substring(0, end)
          val word = if (w.matches("^[\\d\\p{Punct}]+$")) "<num>" else w
          val nature = t.substring(end + 1)
          (word, nature)
        } else ("", "")
      })
      word.filter(t => t._2.startsWith("n") /*|| t._2.startsWith("v")*/).map(_._1)
    }

  }

  private def queryWordVec() = {

    val sqlContext = SparkApp.hiveContext

    Log.info("load word vec start")

    def queryStopWord = {
      val srcSql = "select word from algo.lj_article_word_tfidf where idf <=3 or tf > 165943"
      val srcData = sqlContext.sql(srcSql).map(r => r.getString(0))
      srcData.collect().toSet
    }

    val start1 = System.currentTimeMillis()
    val stopWordBroad = SparkApp.sc.broadcast(queryStopWord)
    val start2 = System.currentTimeMillis()
    val srcSql = s"select word,vec from algo.lj_article_word_vec2 where stat_date='uc170626' and length(word)>1"
    val srcData = sqlContext.sql(srcSql)
      .map(r => {
        val vec = r.getString(1).split(",").map(_.toFloat)
        Blas.norm2(vec)
        (r.getString(0), vec)
      }).filter(r => !stopWordBroad.value.contains(r._1))
    val end = System.currentTimeMillis()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************queryStopWord= " + (end - start1).toString + "*****************************")
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************queryWordVec= " + (end - start2).toString + "*****************************")
    srcData.collect().toMap
  }

  private def queryIdf(): Map[String, Float] = {
    val start = System.currentTimeMillis()
    val srcSql =
      "select word,tf,idf from algo.lj_article_word_tfidf"
    val ret = SparkApp.hiveContext.sql(srcSql).map(r => {
      (r.getString(0), r.getLong(1), r.getDouble(2).toFloat)
    }).map(r => (r._1, r._2 * r._3)).collectAsMap()
    val end = System.currentTimeMillis()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************queryIdf= " + (end - start).toString + "*****************************")
    ret.toMap

  }


  private def segmentArticle(title: String, content: String): (String, String) = {
    val tlist = segment(stripHtml(title))
    val titleWords = if (tlist.isEmpty) "" else tlist.mkString(WORD_SEP)
    var formatedContent =
      if (!isJson(content)) {
        stripHtml(content)
      }
      else {
        extractJsonDesc(content)
      }
    val contentWords = formatedContent.split("\n").filter(!_.trim.isEmpty).map(s => {
      val cList = segment(s)
      if (cList.isEmpty) "" else cList.mkString(WORD_SEP)
    }).mkString(LINE_SEP)
    (titleWords, contentWords)
  }


  def stripHtml(content: String): String = {
    if (StrUtil.isEmpty(content)) ""
    else {
      content.replaceAll("\n", " ").replaceAll("<script>.*?</script>", "")
        .replaceAll("(</p>|</br>)\n*", "\n")
        .replaceAll(htmlPatter, " ")
        .replaceAll("(点击加载图片)|(查看原文)|(图片来源)", "")
        .replaceAll("\\s*\n\\s*", "\n")
        .replaceAll("[ \t]+", " ")
    }
  }

  def isJson(content: String): Boolean = {
    try {
      val j = JSON.parseObject(content)
      j != null
    } catch {
      case e: Exception => false
    }
  }

  def extractJsonDesc(content: String): String = {

    try {
      val json = JSON.parseObject(content)
      if (json != null && json.getJSONArray("content") != null) {
        val ret = new StringBuilder()
        val contents = json.getJSONArray("content")
        for (i <- 0 until contents.size()) {
          val o = contents.getJSONObject(i)
          val desc = if (o != null) o.getString("description") else ""
          ret.append(desc).append("\n")
        }
        return ret.toString()
      }
    } catch {
      case e: Exception => {}
    }
    return ""
  }


  val emptyList = new util.ArrayList[Term]()

  def segment(content: String): util.List[Term] = {

    if (StrUtil.isEmpty(content)) {
      emptyList
    } else {
      val s = content.trim.toLowerCase()
      ArticleSeg.segment(content) //.filter(t=> !t.nature.startsWith("w") /*|| t.word == "\n"*/)
    }
  }


  object ArticleSeg {
    val SEG = HanLP.newSegment()
    //    SEG.enableAllNamedEntityRecognize(true)
    SEG.enableNumberQuantifierRecognize(true)
    SEG.enableCustomDictionary(true)

    def segment(text: String): util.List[Term] = {
      try {
        SEG.seg(HanLP.convertToSimplifiedChinese(text))
      } catch {
        case e: Exception => emptyList
      }
    }

  }


}
