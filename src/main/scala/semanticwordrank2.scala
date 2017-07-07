//package lingc
//
//import utils.{Blas, Log, SparkApp, StrUtil}
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
//
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer
//import org.apache.spark.rdd.RDD
//
///**
//  * Created by xueyuan on 2017/7/4.
//  */
//
//
//object SemanticWordRank2 {
//
//  var srctable_stat_date = "uc170626"
//
//  def main(args: Array[String]): Unit = {
//    var w1 = 1.0
//    var w2 = 1.0
//    var w3 = 1.0
//    if (args.length == 3) {
//      w1 = args(0).toDouble
//      w2 = args(1).toDouble
//      w3 = args(2).toDouble
//    }
//    val id_title_kw = process(w1, w2, w3)
//    val result = id_title_kw.mapPartitions(iter => for (r <- iter) yield {
//      Row(r._1, r._2, r._3.map(r => r._1 + ":" + r._2.toString).mkString(","))
//    })
//
//    val schema = StructType(List(
//      StructField("id", LongType),
//      StructField("title", StringType),
//      StructField("kw", StringType)))
//    SparkApp.saveToHive(result, "algo.xueyuan_article_key", schema, "20170702")
//    //    srcData.unpersist()
//
//    Log.info("finish ******")
//  }
//
//
//  def process(w1: Double, w2: Double, w3: Double): RDD[(Long, String, Array[(String, Double)])] = {
//    val sc = SparkApp.sc
//
//    val wordVecg = queryWordVec()
//    val wordVecBroad = sc.broadcast(wordVecg)
//    val wordIdfBroad = sc.broadcast(queryIdf())
//
//    Log.info("word vec cnt=" + wordVecg.size)
//
//    val srcData = queryArticleData(wordVecg.map(_._1).toSet, false).repartition(400).cache()
//    val expTableBroad = sc.broadcast(new ExpTable)
//
//
//    val id_title_wordsim = srcData.mapPartitions { iter =>
//      for (r <- iter) yield {
//        val title = r._2
//        val content = r._3.flatMap(r => r)
//        val allWords_dup = title ++ content
//        val allWords = allWords_dup.distinct
//        val wordVec = wordVecBroad.value
//        val vec = allWords.map { r =>
//          val vecOption = wordVec.get(r)
//          val vec_in = vecOption match {
//            case Some(s) => s
//            case None => null
//          }
//          (r, vec_in)
//        }.filter(_._2 != null)
//        val wordsim = vec.map { v =>
//          val sim = vec.filter(_._1 != v._1).map(r => Blas.cos(r._2, v._2)).sum
//          (v._1, sim)
//        }
//        (r._1, title, wordsim)
//      }
//    }
//    val id_title_kw = id_title_wordsim.mapPartitions(f = iter => for (r <- iter) yield {
//      val id = r._1
//      val title = r._2
//      val wordsim = r._3
//      val word_sim_tfidf_tit = wordsim.map(r => {
//        val w_tfidf = wordIdfBroad.value
//        val w_set = w_tfidf.keySet
//        val w = r._1
//        val sim = r._2
//        var tfidf = 0.0
//        if (w_set.contains(w)) {
//          tfidf = w_tfidf(w)
//        }
//        var tit = 0.0
//        if (title.contains(w)) {
//          tit = 1
//        }
//        (w, sim, tfidf, tit)
//      })
//      //归一化
//      var sim_max = 0.0
//      var sim_min = 0.0
//      var tfidf_max = 0.0
//      var tfidf_min = 0.0
//      for ((w, sim, tfidf, tit) <- word_sim_tfidf_tit) {
//        if (sim_max < sim) {
//          sim_max = sim
//        } else {
//          sim_min = sim
//        }
//        if (tfidf_max < tfidf) {
//          tfidf_max = tfidf
//        } else {
//          tfidf_min = tfidf
//        }
//      }
//      val sim_div = sim_max - sim_min
//      val tfidf_div = tfidf_max - tfidf_min
//      var word_score = new ArrayBuffer[(String, Double)]()
//      for ((w, sim, tfidf, tit) <- word_sim_tfidf_tit) {
//        word_score += ((w, w1 * (sim - sim_min) / sim_div + w2 * (tfidf - tfidf_min) / tfidf_div + w3 * tit))
//      }
//      val kw = word_score.sortWith(_._2 > _._2).take(3)
//      (id, title.mkString(","), kw.toArray)
//    })
//    id_title_kw
//  }
//
//
//  private def queryWordVec() = {
//    val sqlContext = SparkApp.hiveContext
//
//    Log.info("load word vec start")
//
//    def queryStopWord = {
//      val srcSql = "select word from algo.lj_article_word_tfidf where idf <=3 or tf > 165943"
//      val srcData = sqlContext.sql(srcSql).map(r => r.getString(0))
//      srcData.collect().toSet
//    }
//
//    val stopWordBroad = SparkApp.sc.broadcast(queryStopWord)
//
//    val srcSql = s"select word,vec from algo.lj_article_word_vec2 where stat_date='uc170626' and length(word)>1"
//    val srcData = sqlContext.sql(srcSql)
//      .map(r => {
//        val vec = r.getString(1).split(",").map(_.toFloat)
//        Blas.norm2(vec)
//        (r.getString(0), vec)
//      }).filter(r => !stopWordBroad.value.contains(r._1))
//
//    srcData.collect().toMap
//  }
//
//  def queryArticleData(trainWord: Set[String], test: Boolean) = {
//    val sqlContext = SparkApp.hiveContext
//
//    Log.info("load article start")
//
//    val srcSql =
//      if (test) s"select id,title,content from algo.lj_article_word where stat_date='uc170626' and 1494022103132001 < id and id < 1494037605132008"
//      else s"select id,title,content from algo.lj_article_word where stat_date='uc170626' and id is not null"
//
//
//    val srcData = sqlContext.sql(srcSql).map(r => {
//
//      val id = r.getLong(0)
//      val (title, title_z) = getWords(r.getString(1), trainWord)
//      val con = r.getString(2)
//      val (content,content_z) =
//        if (StrUtil.isEmpty(con)) (Array.empty[Array[String]],Array.empty[Array[String]])
//        else {
//          val data = getWords(con, trainWord)//.filter(!_.isEmpty)
//          data
//        }
//
//      (id, title, content)
//    }).filter(r => !(r._2.isEmpty && r._3.isEmpty))
//    srcData
//  }
//
//  private def queryIdf(): Map[String, Float] = {
//    val srcSql =
//      "select word,tf,idf from algo.lj_article_word_tfidf"
//    val ret = SparkApp.hiveContext.sql(srcSql).map(r => {
//      (r.getString(0), r.getLong(1), r.getDouble(2).toFloat)
//    }).map(r => (r._1, r._2 * r._3)).collectAsMap()
//    ret.toMap
//  }
//
//
//  def getWords(content: String, trainWord: Set[String]) = {
//    if (StrUtil.isEmpty(content)) Array.empty[String]
//    else {
//      val w_n = content.split(',').map(t => {
//        val end = t.lastIndexOf("/")
//        if (end > 0) {
//          val w = t.substring(0, end)
//          val word = if (w.matches("^[\\d\\p{Punct}]+$")) "<num>" else w
//          val nature = t.substring(end + 1)
//          (word, nature)
//        } else ("", "")
//      }).filter(r => trainWord.isEmpty || trainWord.contains(r._1))
//
//      val n = w_n.filter(t => t._2.startsWith("n") && false == (t._2.startsWith("nr") || t._2.startsWith("ns") || t._2.startsWith("nt"))).map(_._1)
//      val nz = w_n.filter(t => t._2.startsWith("nr") || t._2.startsWith("ns") || t._2.startsWith("nt")).map(_._1)
//      (n, nz)
//    }
//
//  }
//
//
//}
//
