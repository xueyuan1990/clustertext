package lingc

import java.util

import com.alibaba.fastjson.JSON
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import utils.{Log, SparkApp, StrUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * Created by tanlingcheng on 2017/6/21.
 */
object Article2Word2 {


  //  def test(): Unit = {
  //    val json = "{\"content\":[{\"description\":\"运动后狂吃蛋白质，运动后狂吃蛋白质可以最大限度提高运动效果�?��?�一直以来，大家都这么认为�?�然而，《国际运动营养学会�?�期刊最近的研究表明，只要你平时日常饮食中蛋白质足够（成年女性每天蛋白质�?佳摄入量�?46g），锻炼后就不需要额外补充蛋白质。\",\"image\":\"http://image.res.meizu.com/image/reader/b185a98a92eba6d3d893e1fb85891c16/original\",\"index\":0},{\"description\":\"运动后可以来�?杯，挺多马拉松赛事的终点都准备了酒让完成比赛的跑者们尽情party，然而，PLOS One期刊的研究表明：运动后喝酒可能会影响肌肉有效恢复和重建的能力。但这并不是说一口都不能喝，如果只来�?杯的话是没有问题的！\",\"image\":\"http://image.res.meizu.com/image/reader/b8eb813701481ab5d2a7d24b709c65ab/original\",\"index\":1},{\"description\":\"运动后大吃特吃犒劳自己， �?常见的补偿心理就是：”我刚刚那么累，现在吃点怎么了？吃得再多，运动掉不就行了吗？“但问题是，这种心理会自然�?�然地蔓延到非运动时段�?�你会慢慢开始这样告诉自己：”我前天跑了10公里，现在吃得多点�?�么了？“这种补偿饮食对瘦小肚子影响�?大�?�对于瘦肚子，你的进食情况比你的锻炼量更重要。\",\"image\":\"http://image.res.meizu.com/image/reader/5d93148573661bbc0dc76ede412703e0/original\",\"index\":2},{\"description\":\"高强度训练之后，不补碳水，一小时以上的高强度运动后（比如跑步或游泳），肌肉需要大量糖原来恢复、变强�?�健康的碳水化合物（如水果�?�蔬菜�?�豆类和全麦食物）是糖原的最佳来源�?? 运动�?60分钟内，进食适量的健康碳水化合物非常必要。这里有�?个公式可以计算： 你需要的碳水化合�?=体重（kg�?*0.001\",\"image\":\"http://image.res.meizu.com/image/reader/764e0d6638c1c8a322f0fa6555d76c9c/original\",\"index\":3},{\"description\":\"不事先计划好吃什么，只要运动的时候好好练了，运动后肯定会特别饿�?�特别饿的时候你会想吃沙拉吗？不，你只想吃披萨�?�麻辣香锅�?�大碗牛肉面......康奈尔大学有研究表明：饥饿的购物者比不太饥饿的购物�?�购买了平均超过46％的高热量物品�?�为了更加理智地做�?�择，聪明的做法是在运动前就决定吃什么�?�\",\"image\":\"http://image.res.meizu.com/image/reader/64da476d6482bee497475c2366caed8a/original\",\"index\":4},{\"description\":\"运动后喝水太少，跑�?�常常低估自己消耗的水量，即使是�?个轻度的锻炼（即使你感觉没有出汗），都可能会导致轻度脱水。如果有条件，可以在锻炼前后称一下自己的体重变化。每减掉1g体重，你�?要补充同等重量的水�?�\",\"image\":\"http://image.res.meizu.com/image/reader/bf75e25e86a389492f8ba50800a8bdd0/original\",\"index\":5}]}"
  //
  //    println(extractJsonDesc(json))
  //
  //    def f1(content:String) ={
  //      var formatedContent =
  //        if (!isJson(content)) {
  //          stripHtml(content)
  //        }
  //        else {
  //          extractJsonDesc(content)
  //        }
  //
  //      val contentWords = formatedContent.split("\n").filter(! _.isEmpty).map(s=>{
  //        val cList = segment(s)
  //        if (cList.isEmpty) "" else cList.mkString(WORD_SEP)
  //      }).mkString(WORD_SEP+"\n"+WORD_SEP)
  //      contentWords
  //    }
  //
  //    val con = "<html>\n <head></head>\n <body>\n  <p>苏里南共和国位于南美洲北部，国名源于当地原住民苏里南人，首都帕拉马里博为苏里南河河口的商港�?�苏里南是一个种族�?�语�?、宗教上极为多元的国家，原为印第安人居住地�?�该国旧称荷属圭亚那，原为荷兰在南美洲的殖民遗迹�?1954年成为荷兰王国海外自治省�?1975年时独立。生活在那里的很多华人称它为“苏里南华人的天堂�?��?�华人的�?位后代陈亚先曾是该国总统�?</p> \n  <img src=\"http://image.res.meizu.com/image/reader/90d0a099692cdf8cadf663b6b289c610/original\" width=\"516\" height=\"387\" /> \n  <p>苏里南无论以面积还是人口排名，都是南美洲�?小的�?个国家，也是西半球不属于荷兰王国组成体的地区中，唯一�?个使用荷兰文为官方语�?者，另外，汉语中的客家语是苏里南共和国的法定语言，旅居苏里南的华人大多籍贯广东客家地区，以旧惠州府为代表，亦有不少广州府客家人，苏里南视惠阳话（客家话）为法定语�?，是世界上少数几个把客家话当成法定语�?的国家，会客家话的小伙伴过去可以不用学外语啦�?</p> \n  <img src=\"http://image.res.meizu.com/image/reader/0564c785bacd319d6448e67daae55617/original\" width=\"509\" height=\"351\" /> \n  <p>另外�?2014�?4月，苏政府将中国农历新年确定为全国永久�?�公共节假日，这在美洲地区尚属首次，实现了几代旅苏侨胞的夙愿。华人移居苏里南已有超过160年的历史，目前在苏华侨华人数量超�?4万，约占全国人口十分之一�?</p> \n  <img src=\"http://image.res.meizu.com/image/reader/babbadabffe8fbb77429310fff74c185/original\" width=\"516\" height=\"340\" /> \n  <p>2016�?3�?1日起，苏里南�?始对中国游客实施免签，不过像古巴�?样，�?要办理一张旅游卡才能入境。苏里南地处赤道热带，北部为热带草原气�?�，南部属热带雨林气候，气�?�湿润，�?以森林繁茂，森林面积占到了全国�?�面积的95%。年平均气温27℃，年降水量�?2000毫米以上。春秋冬三季适宜旅游�?</p> \n  <p>新阿姆斯特丹�?</p> \n  <p>位于科默韦讷河与苏里南河的汇流处，是苏里南北部城市，科默韦讷区首府�?�西南离帕拉马里博约10公里。人�?2�?014。始为荷兰殖民�?�建立的要塞。附近为富饶的农业区，有小型农产品加工工业�?�河港�?�公路�?�帕拉马里博。有古堡改建的露天博物馆，距首都半小时车程�??</p> \n  <img src=\"http://image.res.meizu.com/image/reader/b873b001f34e69482099df1271739320/original\" width=\"521\" height=\"391\" /> \n  <p>布朗斯堡自然公园</p> \n  <p>布朗斯堡自然公园、鸟类特别保护地。据鸟类专家辨认和归档的就有六百多种。住宿在马扎罗尼饭店，该饭店位于范围广阔的阿福巴卡漂亮的景区内�??</p> \n  <img src=\"http://image.res.meizu.com/image/reader/ce40026fa12cdd8e23534cbb2ab41959/original\" width=\"600\" height=\"356\" /> \n  <p>萨拉马卡河上游丛林游</p> \n  <p>萨拉马卡河，发源自威廉明娜山，向北流经大西洋。上游丛林游�?三天两夜，到达原来的金矿镇，在这里可以淘金和钓鱼�?</p> \n  <img src=\"http://image.res.meizu.com/image/reader/f70883bc9d93aad290063104bca1b11a/original\" width=\"599\" height=\"398\" /> \n  <img src=\"http://image.res.meizu.com/image/reader/24b13b144c4259a8c04dff842d12bcbc/original\" width=\"580\" height=\"435\" /> \n  <p>西非村庄</p> \n  <p>西非村庄位于苏里南河上游丛林之中，村里住的是丛林黑人。他们是早期逃跑至丛林的非洲奴隶的后裔，现受政府保护，以适应人类学家研究和旅游的�?要�??</p> \n  <img src=\"http://image.res.meizu.com/image/reader/d87eaec15765b8759ff2e6d0b03fbf5a/original\" width=\"370\" height=\"278\" /> \n  <p>阿尔比纳�?</p> \n  <p>阿尔比纳镇位于马罗尼河下游西岸，是苏里南东北部边境城市和港口，马罗韦讷区首府。附近及内地的木材�?�砂金和稻米、甘蔗等在此集散。港口可停泊中型船只。这里不仅可以见到丛林黑人，而且居住有土著印第安人�?��?�中有时可看到巫毒教火舞�?</p> \n  <img src=\"http://image.res.meizu.com/image/reader/f25bc0834e038b9bb1966623f8597488/original\" width=\"550\" height=\"363\" /> \n  <p>帕拉马里博内城�?��?�最富有创意的古�?</p> \n  <p>位于苏里南帕拉马里博行政区，坐落在苏里南河的西岸。帕拉马里博曾经是十七到十八世纪时的荷兰殖民地�?�帕拉马里博被分�?12个区，里面建筑物数量才超过了1000。帕拉马里博城内古�?�的市中心仍然保持着昔日那富有创意�?�极具特色的街道布局�?</p> \n  <img src=\"http://image.res.meizu.com/image/reader/04f13970157476e021b576e9bb0e8951/original\" width=\"540\" height=\"359\" /> \n  <p>帕拉马里博的建筑中混合了荷兰、德国�?�法国和后来美国的影响，这些影响反映了苏里南历史的发展�?�内城的建筑物依然在向世人展示着荷兰建筑风格与当地传统建筑方法和建筑材料的�?�步融合。由此帕拉马里博获得了它独特的特征�?�建筑物主要以木结构为主，砖用得比较少�?�壮丽的砖结构建筑俯瞰绿色的广场，木制的房子拥挤在狭窄的街道上�?�高耸的棕榈形成林荫道，红树林树立在河岸两旁�?</p> \n  <img src=\"http://image.res.meizu.com/image/reader/82dabb5fccab1fd3df6f2e2a77b03d79/original\" width=\"579\" height=\"385\" /> \n  <p>每次谈到华人很多的国家就特别有亲切感，由此也产生了对这个国家的好感，大概是民族情结吧，�?�么样，想畅通无阻地讲着客家话游玩苏里南吗？</p>\n </body>\n</html>"
  //    println(f1(con))
  //
  //    val content = "i'm buter 3d我们DB332  �?2012�?12�?12日\n\n玩得好开�?2012/12/12"
  //
  //    val a=TimeTokenizer.segment(content)
  //    println(a)
  //    println(a.filter(!_.nature.startsWith("w")))
  //  }

  //add jar hdfs:///share/hive/hive-serde/hive-hcatalog-core-0.13.1.jar;
  //  val srcTable = "mzreader.odl_fdt_algo_article_mq"
  //  val srcTable = "algo.lj_article_src"
  //  val articlWordTable = "algo.lj_article_word"
  //  val tfidfTable = "algo.lj_article_word_tfidf"
  val srcTable = "algo.sw_data2"
  val articlWordTable = "algo.lc_article_word2"
  val tfidfTable = "algo.lc_article_word_tfidf2"
  val htmlPatter = "<[^>]+?>" // "<(/?\\p{Alnum}{1,10})|(\\p{Alnum}{1,10}[^>]+?/?)>"
  val WORD_SEP = ","
  val LINE_SEP =WORD_SEP+"###"+WORD_SEP
  var stat_date = "1"
  var test = true

  def main(args: Array[String]): Unit = {
    //        test()
    //    if(args.length < 2){
    //      println("useage: stat_date")
    //    }
    stat_date=args(0)
    test = args(1).toBoolean
    process()
  }

  private def process() {

    segment("你好�?百万").foreach(println)

    val sqlContext = SparkApp.hiveContext

        val selectSrc =
          if(test) s"select fid,ftitle,fcontent from $srcTable where 1494022103132001 < fid and fid < 1494037605132008"
          else s"select fid,ftitle,fcontent from $srcTable where fid is not null and ftitle is not null and fcontent is not null"

    val srcData = sqlContext.sql(selectSrc).map(r => {
      (r.getString(0).toLong, (r.getString(1), r.getString(2)))
    })
    val srcCount = srcData.count()

    val data = segmentArticle(srcData)

    val articlWord = data.map(d => Row(d._1, d._2, d._3))
    SparkApp.saveToHive(articlWord, articlWordTable, articlWordSchema(),stat_date)

    val docCount = data.count().asInstanceOf[Float]

    val termTfIdf = tfidf(data, docCount).map(d => Row(d._1, d._2, d._3))

    SparkApp.saveToHive(termTfIdf, tfidfTable, wordTfIdfSchema())

    Log.info(s"src count = $srcCount")
    Log.info(s" doc count = $docCount")
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

        val contentWords = formatedContent.split("\n").filter(! _.trim.isEmpty).map(s=>{
          val cList = segment(s)
          if (cList.isEmpty) "" else cList.mkString(WORD_SEP)
        }).mkString(LINE_SEP)
        //          val cList = segment(formatedContent)
        //          val contentWords = if (cList.isEmpty) "" else cList.mkString(WORD_SEP)

        (i._1, titleWords, contentWords)
      })
    })
  }

  private def tfidf(data: RDD[(Long, String, String)], docCount: Float) = {
    data.flatMap(d => {
      val t = d._2
      val c = d._3

      val wordCount = new mutable.HashMap[String, Int].withDefaultValue(0)
      t.split(WORD_SEP).filter(a => a != null && !a.trim.isEmpty).foreach(term => {
        val end = term.lastIndexOf("/")
        if (end > 0) {
          val word = term.substring(0, end)
          wordCount.put(word, wordCount(word) + 1)
        }
      })
      c.split(WORD_SEP).filter(a => a != null && !a.trim.isEmpty).foreach(term => {
        val end = term.lastIndexOf("/")
        if (end > 0) {
          val word = term.substring(0, end)
          wordCount.put(word, wordCount(word) + 1)
        }
      })
      wordCount.map(wc => {
        (wc._1, (wc._2, 1))
      })
    }).reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2)).map(i => {
      (i._1, i._2._1, math.log(docCount / i._2._2))
    })
  }


  def articlWordSchema(): StructType = {
    StructType(
      List(StructField("id", LongType),
        StructField("title", StringType),
        StructField("content", StringType)))
  }

  def wordTfIdfSchema(): StructType = {
    StructType(
      List(StructField("word", StringType),
        StructField("tf", IntegerType),
        StructField("idf", DoubleType)))
  }

  def stripHtml(content: String): String = {
    if (StrUtil.isEmpty(content)) ""
    else {
      content.replaceAll("\n"," ").replaceAll("<script>.*?</script>","")
        .replaceAll("(</p>|</br>)\n*", "\n")
        .replaceAll(htmlPatter, " ")
        .replaceAll("(点击加载图片)|(查看原文)|(图片来源)", "")
        .replaceAll("\\s*\n\\s*", "\n")
        .replaceAll("[ \t]+", " ")
    }
  }

  def isJson(content: String): Boolean = {
    //    content.contains("\"content\":") &&
    //      content.contains("\"description\":\"") &&
    //      content.contains("\"image\":\"")
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
      ArticleSeg.segment(content)//.filter(t=> !t.nature.startsWith("w") /*|| t.word == "\n"*/)
    }
  }


  object ArticleSeg {
    val SEG = HanLP.newSegment()
    //    SEG.enableAllNamedEntityRecognize(true)
    SEG.enableNumberQuantifierRecognize(true)
    SEG.enableCustomDictionary(true)

    def segment(text: String): util.List[Term] = {
      try{
        SEG.seg(HanLP.convertToSimplifiedChinese(text))
      }catch {
        case e:Exception => emptyList
      }
    }

  }

  //  object TimeTokenizer {
  //    val SEG = HanLP.newSegment()
  //    val pat = Pattern.compile("(\\d+-?\\p{Alpha}+\\d*)|(\\p{Alpha}+-?\\d+\\p{Alpha}*)" +
  //      "|(\\d{2,4}�?)|\\d{1,2}(月|日|点|时|分|�?)|\\d{4}([:/\\-]\\d{1,2}){1,2}")
  //
  //    def segment(text: String): util.List[Term] = {
  //      val termList = new util.ArrayList[Term]()
  //      val matcher = pat.matcher(text)
  //
  //      var begin = 0
  //      var end = 0
  //      while (matcher.find()) {
  //        end = matcher.start()
  //
  //        termList.addAll(SEG.seg(text.substring(begin, end)));
  //        termList.add(new Term(matcher.group(), Nature.n));
  //        begin = matcher.end();
  //      }
  //      if (begin < text.length()) termList.addAll(SEG.seg(text.substring(begin)));
  //
  //      termList
  //    }
  //  }

}

