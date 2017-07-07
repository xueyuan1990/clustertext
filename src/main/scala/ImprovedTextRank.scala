package lingc

import utils.{Blas, Log, SparkApp, StrUtil}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, IntegerType, StringType, StructField, StructType}

import scala.collection.mutable

/**
  * Created by linjiang on 2017/5/8.
  */
class RankConf extends Serializable {
  var test = false
  var batchTest = false
  var useExp = false
  var windowSize = 5
  var damp = 0.85
  var iter = 20
  //  var stat_date = "1"

//  var weightType = -1
  var topN = 4

  var titleWeight = 10.0

  //  var alpha = 0.1
  //  var beta = 0.9
  //  val gama = 0.0

  val minScoreDif = 0.0001

  var weighConf: Map[String, Int] = null

  //  override def toString: String = s"useage : test=$test, iter=$iter, window=$windowSize,damp=$damp, stat_date=$stat_date," +
  //    s"weitype=$weightType,topN=$topN,alpha=$alpha,beta=$beta"
  override def toString: String =
  s"useage : batch=$batchTest test=$test, iter=$iter, window=$windowSize, damp=$damp, titleWeight=$titleWeight" +
    weighConf.toString()
}

class TxtRanker(conf: RankConf,
                title: Array[String], content: Array[Array[String]],
                wordVec: Map[String, Array[Float]], expTable: ExpTable,
                wordIdf: Map[String, Float] = Map.empty.withDefaultValue(1)) {

  private val wordGraph = buildWordGraph()
  private val wordCnt = wordGraph.size
  private val wordPositionWeight: Map[String, Float] = {
    val ret = new mutable.HashMap[String, Float]()

    content.foreach(sen => {
      sen.foreach(w => {
        ret += w -> 1.0f
      })
    })

    title.foreach(w => {
      ret += w -> conf.titleWeight.toFloat
    })
    ret.toMap
  }
  private val wordOutPositionWeightSum: Map[String, Float] = {
    wordGraph.map(item => {
      val w = item._1
      val weighSum = item._2.map(e => wordPositionWeight(e)).sum
      (w, weighSum)
    })
  }

  private val wordOutIdfWeightSum: Map[String, Float] = {
    wordGraph.map(e => {
      val w = e._1
      val weighSum = e._2.map(e2 => wordIdf(e2)).sum
      (w, weighSum)
    })
  }

  private val simMap = new mutable.HashMap[(String, String), Double]()

  private def similarity(w1: String, w2: String): Double = {
    val key = if (w1 > w2) (w2, w1) else (w1, w2)
    if (!simMap.contains(key)) {
      var f = Blas.dot(wordVec(w1), wordVec(w2))
      if (conf.useExp) {
        f = expTable.calc(f)
      }
      simMap.put(key, f)
    }
    simMap(key)
  }

  val wFunMap = Map(
    "s" -> simWeight _,
    "u" -> uniformWeight,
    "p" -> positionWeight _,
    "i" -> idfWeight _
  )

  // w1 -> w2
  private def simWeight(w1: String, w2: String): Double = {
    similarity(w1, w2) / wordGraph(w1).length //  |out(w1)|
  }

  // w1 -> w2
  private def uniformWeight = (w1: String, w2: String) => 1.0 / wordGraph(w1).length

  // w1 -> w2
  private def positionWeight(w1: String, w2: String): Double = {
    wordPositionWeight(w2) / wordOutPositionWeightSum(w1)
  }

  private def idfWeight(w1: String, w2: String): Double = {
    wordIdf(w2) / wordOutIdfWeightSum(w1)
  }

  /* private val weight = conf.weightType match {
     case 1 => uniformWeight
     case 2 => simWeight _
     case 3 => positionWeight _
     case 4 => (w1: String, w2: String) => conf.alpha * uniformWeight(w1, w2) + conf.beta * positionWeight(w1, w2)
     case 5 => (w1: String, w2: String) => conf.alpha * simWeight(w1, w2) + conf.beta * positionWeight(w1, w2)

     case 6 => (w1: String, w2: String) => conf.alpha * uniformWeight(w1, w2) + conf.beta * idfWeight(w1, w2)
     case 7 => (w1: String, w2: String) => conf.alpha * simWeight(w1, w2) + conf.beta * idfWeight(w1, w2)

     case 8 => (w1: String, w2: String) => conf.alpha * simWeight(w1, w2) + conf.beta * positionWeight(w1, w2) + conf.gama * idfWeight(w1, w2)
     case _ => throw new RuntimeException(s"unknow weight type $conf.weightType")
   }*/

  private def buildWordGraph(): Map[String, Array[String]] = {

    val wordsLinks = new mutable.HashMap[String, mutable.Set[String]]().withDefaultValue(new mutable.HashSet[String]())

    title.foreach(w => wordsLinks += w -> (mutable.HashSet(title: _*) -= w))

    content.foreach(s => {
      s.sliding(conf.windowSize).foreach(window => {
        for (i <- 0 until window.length) {
          val word = window(i)
          val linkNodes = new mutable.HashSet[String]
          linkNodes ++= window.slice(0, i)
          linkNodes ++= window.slice(i + 1, window.length)
          wordsLinks.put(word, (wordsLinks(word) | linkNodes) - word)
        }
      })
    })

    wordsLinks.map(i => i._1 -> i._2.toArray).toMap
  }

  val weightDenominator = conf.weighConf.filter(_._2 > 0).map(_._2).sum.toFloat
  val weiConf = conf.weighConf.filter(_._2 > 0).mapValues(_ / weightDenominator)

  def calcWeight(w1: String, w2: String): Double = {
    weiConf.map(e => e._2 * wFunMap(e._1)(w1, w2)).sum
  }

  var iterCnt = 0

  def calcScore(): Array[(String, Double)] = {
    var score = Map[String, Double]().withDefaultValue(1.0)
    var maxScoreDif = Double.MaxValue
    var i = 0
    while (maxScoreDif > conf.minScoreDif && i < conf.iter && !wordGraph.isEmpty) {
      var newScore = wordGraph.map(wlink => {
        val w1 = wlink._1
        val w1score = wlink._2.map(w2 => {
          score(w2) * calcWeight(w2, w1)
        }).sum
        (w1, (1 - conf.damp) + conf.damp * w1score)
      })
      maxScoreDif = newScore.map(e => math.abs(e._2 - score(e._1))).max
      score = newScore
      i += 1
    }
    iterCnt = i
    score.toArray.sortWith(_._2 > _._2).take(conf.topN)
  }

  //  private def debug(score: Map[String, Double]): Unit = {
  //    if (Log.debugEnable) {
  //      val fmt = "%.2f"
  //      wordGraph.foreach(wl => {
  //        val w1 = wl._1
  //        val s = score(w1)
  //        val ws = wl._2.map(w2 => conf.weightType match {
  //          case 1 => s"%s:u-$fmt".format(w2, uniformWeight(w2, w1))
  //          case 2 => s"%s:s-$fmt".format(w2, simWeight(w2, w1))
  //          case 3 => s"%s:p-$fmt".format(w2, positionWeight(w2, w1))
  //          case 4 => s"%s:up-$fmt(u-$fmt,p-$fmt)".format(w2, conf.alpha * uniformWeight(w2, w1) + conf.beta * positionWeight(w2, w1), conf.alpha * uniformWeight(w2, w1), conf.beta * positionWeight(w2, w1))
  //          case 5 => "%s:sp-" + conf.alpha * simWeight(w2, w1) + conf.beta * positionWeight(w2, w1)
  //        }).mkString(",")
  //        Log.debug("%s:%.4f ==>> %s".format(w1, s, ws))
  //      })
  //      Log.debug("\n")
  //      Log.debug("==" * 50)
  //      Log.debug("\n")
  //    }
  //  }
}

class ExpTable extends Serializable {
  private val EXP_TABLE_SIZE = 1000
  private val MAX_EXP = 6

  private val expTab = createExpTable()

  private def createExpTable(): Array[Float] = {
    val expTable = new Array[Float](EXP_TABLE_SIZE)
    var i = 0
    while (i < EXP_TABLE_SIZE) {
      val tmp = math.exp((2.0 * i / EXP_TABLE_SIZE - 1.0) * MAX_EXP)
      expTable(i) = (tmp / (tmp + 1.0)).toFloat
      i += 1
    }
    expTable
  }

  def calc(f: Double): Float = {
    val ind = if (-MAX_EXP >= f) 0
    else if (f >= MAX_EXP) EXP_TABLE_SIZE - 1
    else { //if (-MAX_EXP < f && f < MAX_EXP)
      ((f + MAX_EXP) * (EXP_TABLE_SIZE / MAX_EXP / 2.0)).toInt
    }
    expTab(ind)
  }
}

object ImprovedTextRank {

  var srctable_stat_date = "1"

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      println("useage : srctable_stat_date, test, topN, windown-size, [weight-alpha_]*")
      return
    }

    val rankConf = new RankConf
    var i = 0
    srctable_stat_date = args(i)
    i += 1
    rankConf.test = args(i).toBoolean
    i += 1
    rankConf.topN = args(i).toInt
    i += 1
    rankConf.windowSize = args(i).toInt

    if (args.length < 5) rankConf.batchTest = true
    else {
      rankConf.batchTest = false
      rankConf.weighConf = args(4).trim.split("_").map(s => (s.substring(0, 1), s.substring(2).toInt)).toMap
    }
    process(rankConf)

  }

  def makeStatDate(weiconf: Map[String, Int], test: Boolean): String = {
    weiconf.filter(_._2 > 0).toArray.sortBy(_._1).map(e => e._1 + e._2).mkString("_") + (if (test) "_1" else "_2")
  }

  def makeWeightConfs(rankConf: RankConf): IndexedSeq[Map[String, Int]] = {
    if (rankConf.batchTest) {
      (0 to 10 by 2).flatMap(a => {
        (0 to (10 - a) by 2).flatMap(b => {
          val c = 10 - a - b
          if (a > 0) {
            List("u", "s").map(a1 => {
              val wc = new mutable.HashMap[String, Int]
              wc += a1 -> a
              if (b > 0) wc += "p" -> b;
              if (c > 0) wc += "i" -> c;
              wc.toMap
            })
          } else {
            val wc = new mutable.HashMap[String, Int]
            if (b > 0) wc += "p" -> b;
            if (c > 0) wc += "i" -> c;
            List(wc.toMap)
          }
        })
      })
    } else {
      Array(rankConf.weighConf).toIndexedSeq
    }
  }

  def process(rankConf: RankConf): Unit = {
    val sc = SparkApp.sc

    val wordVecg = queryWordVec()
    val wordVecBroad = sc.broadcast(wordVecg)
    val wordIdfBroad = sc.broadcast(queryIdf())

    Log.info("word vec cnt=" + wordVecg.size)

    val srcData = queryArticleData(wordVecg.map(_._1).toSet, rankConf.test).repartition(200).cache()
    val expTableBroad = sc.broadcast(new ExpTable)

    val schema = StructType(List(
      StructField("id", LongType),
      StructField("title", StringType),
      StructField("kw", StringType),
      StructField("it", IntegerType)))

    val confs = makeWeightConfs(rankConf)

    confs.foreach(cw => {
      rankConf.weighConf = cw
      val conf = rankConf
      Log.info(conf.toString)
      val rtRDD = srcData.mapPartitions(it => {
        val wordVec = wordVecBroad.value
        val expTable = expTableBroad.value
        it.map(r => {
          val txtRanker = new TxtRanker(conf, r._2, r._3, wordVec, expTable, wordIdfBroad.value.withDefaultValue(1))
          val scores = txtRanker.calcScore()
          Row(r._1, r._2.mkString(","), scores.map(kw => kw._1 + (":%.4f".format(kw._2))).mkString(","), txtRanker.iterCnt)
        })
      })
      val stat_date = makeStatDate(cw, conf.test)
      SparkApp.saveToHive(rtRDD, "algo.lj_article_key", schema, stat_date)
    })

    srcData.unpersist()

    Log.info("finish ******")
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

    val srcSql = s"select word,vec from algo.lj_article_word_vec2 where stat_date=$srctable_stat_date and length(word)>1 "
    val srcData = sqlContext.sql(srcSql)
      .map(r => {
        val vec = r.getString(1).split(",").map(_.toFloat)
        Blas.norm2(vec)
        (r.getString(0), vec)
      }).filter(r => !stopWordBroad.value.contains(r._1))

    srcData.collect().toMap
  }

  def queryArticleData(trainWord: Set[String], test: Boolean) = {
    val sqlContext = SparkApp.hiveContext

    Log.info("load article start")

//    val srcSql =
//      if (test) s"select id,title,content from algo.lj_article_word where stat_date=3 and 1494022103132001 < id and id < 1494037605132008"
//      else s"select id,title,content from algo.lj_article_word where stat_date=3 and id is not null"

    val srcSql =
      if (test) s"select id,title,content from algo.lj_article_word where stat_date=3 and 1494022103132001 < id and id < 1494037605132008"
      else s"select id,title,content from algo.lj_article_word where stat_date=3 and id is not null"

    val trainWordBroad = SparkApp.sc.broadcast(trainWord)

    val srcData = sqlContext.sql(srcSql).map(r => {
      val trainWords = trainWordBroad.value
      val id = r.getLong(0)
      val title = getWords(r.getString(1)).filter(trainWords.isEmpty || trainWords.contains(_))
      val con = r.getString(2)
      val content =
        if (StrUtil.isEmpty(con)) Array.empty[Array[String]]
        else con.split(',')
          .map(getWords(_).filter(trainWords.isEmpty || trainWords.contains(_))).filter(!_.isEmpty)

      (id, title, content)
    }).filter(r => !(r._2.isEmpty && r._3.isEmpty))
    srcData
  }

  private def queryIdf(): Map[String, Float] = {
    val srcSql =
      s"select word,idf from algo.lj_article_word_tfidf"

    val ret = SparkApp.hiveContext.sql(srcSql).map(r => {
      (r.getString(0), r.getDouble(1).toFloat)
    }).collectAsMap()
    ret.toMap
  }


  def getWords(content: String) = {
    if (StrUtil.isEmpty(content)) Array.empty[String]
    else
      content.split(',').map(t => {
        val end = t.lastIndexOf("/")
        if (end > 0) {
          val w = t.substring(0, end)
          val word = if (w.matches("^[\\d\\p{Punct}]+$")) "<num>" else w
          val nature = t.substring(end + 1)
          (word, nature)
        } else ("", "")
      }).filter(t => t._2.startsWith("n") /*|| t._2.startsWith("v")*/).map(_._1)
  }


}
