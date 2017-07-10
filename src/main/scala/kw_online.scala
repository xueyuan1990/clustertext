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
object kw_online {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val htmlPatter = "<[^>]+?>"
  val WORD_SEP = ","
  val LINE_SEP = WORD_SEP + "###" + WORD_SEP
  var count = 100

  def main(args: Array[String]): Unit = {
    var w1 = 1.0
    var w2 = 1.0
    var w3 = 1.0
    if (args.length == 4) {
      w1 = args(0).toDouble
      w2 = args(1).toDouble
      w3 = args(2).toDouble
      count = args(3).toInt
    }
    var title = ""
    var content = ""
    val data = get_segment_words()
    data.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************data= " + data.count() + "*****************************")
    val sc = SparkApp.sc
    //    val data = sc.parallelize(Array((1L, (title, content))))
    val start = System.currentTimeMillis()
    val se = process(data, w1, w2, w3)
    val id_title_content_kw = se.mapPartitions(iter => for (r <- iter) yield {
      (r._1, r._2, r._3, r._4.map(r => r._1 + ":" + r._2.toString).mkString(","))
    })
    id_title_content_kw.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************id_title_content_kw= " + id_title_content_kw.count() + "*****************************")
    data.unpersist()

    val result = id_title_content_kw.mapPartitions(iter => for (r <- iter) yield {
      Row(r._1, r._2, r._3, r._4)
    })
    result.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************result= " + result.count() + "*****************************")
    val time = System.currentTimeMillis() - start
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************time= " + time + "*****************************")
    id_title_content_kw.unpersist()
    val schema = StructType(List(
      StructField("id", LongType),
      StructField("title", StringType),
      StructField("content", StringType),
      StructField("kw", StringType)))
    SparkApp.saveToHive(result, "algo.xueyuan_article_key_online", schema, "20170704")
    //    srcData.unpersist()

    Log.info("finish ******")
  }

  def process(data: RDD[(Long, (String, String))], w1: Double, w2: Double, w3: Double): RDD[(Long, String, String, Array[(String, Double)])] = {

    val sc = SparkApp.sc
    val wordVecg = queryWordVec()
    val wordVecBroad = sc.broadcast(wordVecg)
    val wordIdfBroad = sc.broadcast(queryIdf())
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************wordVecg= " + wordVecg.size + "*****************************")
    Log.info("word vec cnt=" + wordVecg.size)
    val trainWordBroad = sc.broadcast(wordVecg.map(_._1).toSet)
    val se = segmentArticle(data.map(r=>(r._1,(r._2._1.toLowerCase(),r._2._2.toLowerCase()))))

    //    println(se.count())
    //    se
    val id_title_content2_content = se.map(r => {
      val trainWords = trainWordBroad.value
      val id = r._1
      val title = getWords(r._2).filter(trainWords.isEmpty || trainWords.contains(_))
      val content = r._3
      val content2 =
        if (StrUtil.isEmpty(content)) {
          Array.empty[String]
        }
        else {
          (getWords(content).filter(trainWords.isEmpty || trainWords.contains(_)))
        }

      (id, title, content2, content)
    }).filter(r => !(r._2.isEmpty && (r._3).isEmpty))

    //    val srcData = queryArticleData(wordVecg.map(_._1).toSet, false).repartition(400).cache()
    val expTableBroad = sc.broadcast(new ExpTable)


    val id_title_content_wordsim = id_title_content2_content.mapPartitions { iter =>
      for (r <- iter) yield {
        val title = r._2
        val content2 = r._3
        val allWords_dup = title ++ content2
        val allWords = allWords_dup.distinct
        val wordVec = wordVecBroad.value
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
        (r._1, title, r._4, wordsim)
      }
    }
    val id_title_content_kw = id_title_content_wordsim.mapPartitions(f = iter => for (r <- iter) yield {
      val id = r._1
      val title = r._2
      val content = r._3
      val wordsim = r._4
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
      val kw = word_score.sortWith(_._2 > _._2).take(3)
      (id, title.mkString(","), content, kw.toArray)
    })
    id_title_content_kw
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

    val stopWordBroad = SparkApp.sc.broadcast(queryStopWord)

    val srcSql = s"select word,vec from algo.lj_article_word_vec2 where stat_date='uc170626' and length(word)>1"
    val srcData = sqlContext.sql(srcSql)
      .map(r => {
        val vec = r.getString(1).split(",").map(_.toFloat)
        Blas.norm2(vec)
        (r.getString(0), vec)
      }).filter(r => !stopWordBroad.value.contains(r._1))

    srcData.collect().toMap
  }

  private def queryIdf(): Map[String, Float] = {
    val srcSql =
      "select word,tf,idf from algo.lj_article_word_tfidf"
    val ret = SparkApp.hiveContext.sql(srcSql).map(r => {
      (r.getString(0), r.getLong(1), r.getDouble(2).toFloat)
    }).map(r => (r._1, r._2 * r._3)).collectAsMap()
    ret.toMap
  }

  private def get_segment_words(): RDD[(Long, (String, String))] = {
    val srcTable = "mzreader.ods_t_article_c"
    val test = false
    val sqlContext = SparkApp.hiveContext
    val selectSrc =
      if (test) s"select fid,ftitle,cast(unbase64(fcontent) as string) from $srcTable where (1494022103132001 < fid and fid < 1494037605132008) or fid=1496204576132002" //1496204576132002
      else s"select a.fid,a.ftitle,cast(unbase64(a.fcontent) as string) from $srcTable a, algo.xueyuan_article_key b where b.stat_date='20170703' and a.fid=b.id and a.fresource_type=2  and a.fid is not null and a.fkeywords is not null and a.fkeywords != '' limit " + count
    val srcData = sqlContext.sql(selectSrc)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************srcData= " + srcData.count() + "*****************************")
    val id_title_content = srcData.map(r => {
      (r.getLong(0), (r.getString(1), r.getString(2)))
    })
    id_title_content

  }


  private def segmentArticle(srcData: RDD[(Long, (String, String))]) = {
    srcData
      .repartition(200).reduceByKey((a, b) => a)
      .mapPartitions(it => {
        it.map(i => {
          val tlist = segment(stripHtml(i._2._1))
          val titleWords = if (tlist.isEmpty) "" else tlist.mkString(WORD_SEP)

          val content = i._2._2
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
          (i._1, titleWords, contentWords)
        })
      })
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
