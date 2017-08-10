package keyword

import java.text.SimpleDateFormat
import java.util

import com.alibaba.fastjson.JSON
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import utils.{Blas, Log, SparkApp, StrUtil}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/7/4.
  */
object KeyWord {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val htmlPatter = "<[^>]+?>"
  val WORD_SEP = ","
  val LINE_SEP = WORD_SEP + "###" + WORD_SEP
  var count = 100

  private def seg(title: String, content: String): (Array[String], Array[String]) = {
    val se = segmentArticle(title.toLowerCase(), content.toLowerCase())
    val title_se = getWords(se._1)
    val content_se =
      if (StrUtil.isEmpty(se._2)) {
        Array.empty[String]
      }
      else {
        (getWords(se._2))
      }
    (title_se, content_se)

  }

  private def seg_dis(title: String, content: String): Array[String] = {
    val (title_se, content_se) = seg(title, content)
    (title_se ++ content_se).distinct
  }

  def get_kw(title_in: String, content: String, wordVec: Map[String, Array[Double]], wordIdf: Map[String, Double], weight: Array[Double]): Array[String] = {
    var title = title_in
    if (content == null || wordVec == null || wordIdf == null || weight == null || weight.length < 3) {
      Array.empty[String]
    } else {
      if (title_in == null) {
        title = ""
      }
      val trainWords = wordVec.map(_._1).toSet & wordIdf.keySet
      val (title_se, content_se) = seg(title, content)
      val title_se2 = title_se.filter(trainWords.isEmpty || trainWords.contains(_))
      val content_se2 = content_se.filter(trainWords.isEmpty || trainWords.contains(_))
      val allWords_dup = title_se2 ++ content_se2
      val allWords = allWords_dup.distinct
      //此处tf仅为词出现次数，并没有除以文章总词数，但不影响结果
      var w_tf = allWords.map(r => (r, 0)).toMap
      for (w <- allWords_dup) {
        val tf = w_tf(w)
        val c = tf + 1
        w_tf += ((w, c))
      }
      val vec = allWords.map { r =>
        val vecOption = wordVec.get(r)
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
        val w = r._1
        val sim = r._2
        val idf = wordIdf(w)
        var tit = 0.0
        if (title_se2.contains(w)) {
          tit = 1
        }
        (w, sim, idf * w_tf(w), tit)
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
        word_score += ((w, weight(0) * (sim - sim_min) / sim_div + weight(1) * (tfidf - tfidf_min) / tfidf_div + weight(2) * tit))
      }

      val kw = new ArrayBuffer[String]()
      for ((w, s) <- word_score.sortWith(_._2 > _._2).take(3)) {
        kw += w + ":1"
      }
      kw.toArray
    }
  }


  private def getWords(content: String) = {
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


  def queryWordVec(stat_date: String) = {
    val srcSql = "select word from algo.lc_article_word_tfidf where idf <=3 or tf > 165943 and tf is not null and idf is not null"
    val srcData = SparkApp.hiveContext.sql(srcSql).map(r => r.getString(0))
    val stopWord = srcData.collect().toSet

    Log.info("load word vec start")
    //    val stopWordBroad = SparkApp.sc.broadcast(stopWord)
    val srcSql_2 = s"select word,vec from algo.lj_article_word_vec2 where stat_date='" + stat_date + "'"
    val srcData_2 = SparkApp.hiveContext.sql(srcSql_2)
      .map(r => {
        val vec = r.getString(1).split(",").map(_.toDouble)
        Blas.norm3(vec)
        (r.getString(0), vec)
      }).filter(r => !stopWord.contains(r._1))
    srcData_2.collect().toMap
  }

  def queryIdf(): Map[String, Double] = {
    val srcSql =
      "select word,idf from algo.lc_article_word_tfidf where tf is not null and idf is not null"
    val ret = SparkApp.hiveContext.sql(srcSql)
    val r = ret.map(r => {
      (r.getString(0), r.getDouble(1))
    }).map(r => (r._1, r._2)).collectAsMap()
    r.toMap
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
      //            content.replaceAll("\n", " ").replaceAll("<script>.*?</script>", "")
      //              .replaceAll("(</p>|</br>)\n*", "\n")
      //              .replaceAll(htmlPatter, " ")
      //              .replaceAll("(点击加载图片)|(查看原文)|(图片来源)", "")
      //              .replaceAll("\\s*\n\\s*", "\n")
      //              .replaceAll("[ \t]+", " ")
      content.replaceAll("\n", " ").replaceAll("<script>.*?</script>", "")
        .replaceAll("(</p>|</br>)\n*", "\n")
        .replaceAll("<[^>]+?>", " ")
        .replaceAll("(点击加载图片)|(查看原文)|(图片来源)", " ")
        .replaceAll("\\s*\n\\s*", "\n")
        .replaceAll("[ \t]+", " ")
        .replaceAll("\n", " ")
    }
  }

  private def isJson(content: String): Boolean = {
    try {
      val j = JSON.parseObject(content)
      j != null
    } catch {
      case e: Exception => false
    }
  }

  private def extractJsonDesc(content: String): String = {

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

  private def segment(content: String): util.List[Term] = {

    if (StrUtil.isEmpty(content)) {
      emptyList
    } else {
      val s = content.trim.toLowerCase()
      ArticleSeg.segment(content) //.filter(t=> !t.nature.startsWith("w") /*|| t.word == "\n"*/)
    }
  }


  private object ArticleSeg {
    val SEG = HanLP.newSegment()
    //    SEG.enableAllNamedEntityRecognize(true)
    SEG.enableNumberQuantifierRecognize(true)
    SEG.enableCustomDictionary(true)
    SEG.enableNameRecognize(false)

    def segment(text: String): util.List[Term] = {
      try {
        SEG.seg(HanLP.convertToSimplifiedChinese(text))
      } catch {
        case e: Exception => emptyList
      }
    }

  }


}
