package keyword

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.github.fommil.netlib.F2jBLAS
import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import lingc.ExpTable
import org.apache.hadoop.fs.Path
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.mllib.optimization.SquaredL2Updater
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import utils.{Blas, Log, SparkApp, StrUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by xueyuan on 2017/7/4.
  * 词位置（标题，首段）
  * 词标题相似度：标题中词相加
  * 词和代表词相似度
  * 文章分类
  */
object uc_lr {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  val htmlPatter = "<[^>]+?>"
  val WORD_SEP = ","
  val LINE_SEP = WORD_SEP + "###" + WORD_SEP
  var count = 0
  val partitionnum = 400

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
    if (hiveContext == null) {
      println("***********************hive is null*****************************")
    } else {
      println("***********************hive ok*****************************")
    }
    val t0 = System.currentTimeMillis()
    val word_vec = queryWordVec()
    val t01 = System.currentTimeMillis()
    println("train time1=============30s:" + (t01 - t0))
    val word_idf = queryIdf()
    val t02 = System.currentTimeMillis()
    println("train time1=============27s:" + (t02 - t01))
    val word_vec_br = sc.broadcast(word_vec)
    val t03 = System.currentTimeMillis()
    println("train time1=============8s:" + (t03 - t02))
    val word_idf_br = sc.broadcast(word_idf)
    val t04 = System.currentTimeMillis()
    println("train time1=============84s:" + (t04 - t03))
    val id_kw_title_content_p1_cate = load_data().cache()
    val t1 = System.currentTimeMillis()
    println("train time1=============3s:" + (t1 - t04))
    val id_kw_title_content_p1_cate2 = id_kw_title_content_p1_cate.repartition(partitionnum).mapPartitions(iter => for (r <- iter) yield {
      val word_vec_key = word_vec_br.value.keySet
      val word_idf_key = word_idf_br.value.keySet
      var title = new ArrayBuffer[String]()
      for (tit <- r._3) {
        //remove the word not in vec , idf
        if (word_vec_key.contains(tit) && word_idf_key.contains(tit)) {
          title += tit
        }
      }
      var content = new ArrayBuffer[String]()
      for (cont <- r._4) {
        //remove the word not in vec , idf
        if (word_vec_key.contains(cont) && word_idf_key.contains(cont)) {
          content += cont
        }
      }

      val kw = new ArrayBuffer[String]()
      //remove the word not in vec , idf , content and title
      for (w <- r._2) {
        if (title.contains(w) || content.contains(w)) {
          kw += w
        }
      }
      (r._1, kw.toArray, title.toArray, content.toArray, r._5, r._6)
    })
    id_kw_title_content_p1_cate2.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************id_kw_title_content_p1_cate2_size" + id_kw_title_content_p1_cate2.count() + "*****************************")
    id_kw_title_content_p1_cate.unpersist()
    val t2 = System.currentTimeMillis()
    println("train time2:196s" + (t2 - t1))
    //mapping cate and author to a int value
    val cate = id_kw_title_content_p1_cate2.map(_._6).distinct.zipWithIndex().collect()
    val catesize = cate.length
    val cate_map = cate.map(r => {
      val c = r._1
      val i = r._2
      var result = Array.fill(catesize)(0.0)
      result(i.toInt) = 1.0
      (c, result)
    }).toMap
    val t3 = System.currentTimeMillis()
    println("train time3:1s" + (t3 - t2))
    val cate_map_br = sc.broadcast(cate_map)

    val id_kw_title_content_p1_cate3 = id_kw_title_content_p1_cate2.mapPartitions(iter => for (r <- iter) yield {
      (r._1, r._2, r._3, r._4, r._5, cate_map_br.value(r._6))
    })
    id_kw_title_content_p1_cate3.cache()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************id_kw_title_content_p1_cate3_size" + id_kw_title_content_p1_cate3.count() + "*****************************")
    id_kw_title_content_p1_cate2.unpersist()
    val t4 = System.currentTimeMillis()
    println("train time4:38s" + (t4 - t3))
    //1 label 正样本 关键词
    val label_feature_1 = get_feature(id_kw_title_content_p1_cate3, word_idf_br, word_vec_br, 1)
    label_feature_1.cache()

    //0 label 负样本 非关键词
    val label_feature_0_src = id_kw_title_content_p1_cate3.mapPartitions(iter => for (r <- iter) yield {
      val kw = r._2
      val title = r._3.filter(r => kw.contains(r) == false)
      val content = r._4.filter(r => kw.contains(r) == false)
      (r._1, (title ++ content).distinct, r._3, r._4, r._5, r._6)
    })

    val label_feature_0 = get_feature(label_feature_0_src, word_idf_br, word_vec_br, 0)
    label_feature_0.cache()
    println("label0_size=" + label_feature_0.count() + ",label1_size=" + label_feature_1.count())
    id_kw_title_content_p1_cate3.unpersist()
    val t5 = System.currentTimeMillis()
    println("train time5:1070s" + (t5 - t4))
    val feature = label_feature_0.map(r => r._2) ++ label_feature_1.map(r => r._2)
    val (tit_sim_point, tfidf_point) = mapping(feature.map(r => (r._3, r._4)).collect())
    val t6 = System.currentTimeMillis()
    println("train time6:300s" + (t6 - t5))
    val tit_sim_point_br = sc.broadcast(tit_sim_point)
    val tfidf_point_br = sc.broadcast(tfidf_point)
    //(label, (in_title, in_p1, tit_sim, tf * idf, cate))
    val label_feature_00 = label_feature_0.mapPartitions(iter => for (r <- iter) yield {
      val label = r._1
      val feature = (r._2._1 ++ r._2._2 ++ tran(r._2._3, tit_sim_point_br.value) ++ tran(r._2._4, tfidf_point_br.value) ++ r._2._5)
      val dv = new DenseVector(feature)
      new LabeledPoint(label, dv)
    })
    val label_feature_11 = label_feature_1.mapPartitions(iter => for (r <- iter) yield {
      val label = r._1
      val feature = (r._2._1 ++ r._2._2 ++ tran(r._2._3, tit_sim_point_br.value) ++ tran(r._2._4, tfidf_point_br.value) ++ r._2._5)
      val dv = new DenseVector(feature)
      new LabeledPoint(label, dv)
    })
    val t7 = System.currentTimeMillis()
    println("train time7:0s" + (t7 - t6))
    training_lr(label_feature_00, label_feature_11)
    val t8 = System.currentTimeMillis()
    println("train time8:" + (t8 - t7))
  }

  def mapping(feature: Array[(Double, Double)]) = {
    //统计取值范围和各个取值段的比例

    val tit_sim = feature.map(r => r._1).sortWith(_ < _)
    val tfidf = feature.map(r => r._2).sortWith(_ < _)

    val size = tit_sim.length
    val groups = 6
    val group_size = size.toDouble / groups

    var tit_sim_point = new ArrayBuffer[Double]()
    var tfidf_point = new ArrayBuffer[Double]()

    for (index <- 1 until groups) {
      val i = (index * group_size).toInt
      tit_sim_point += tit_sim(i)
      tfidf_point += tfidf(i)
    }

    //    size: 299263
    //    tit_sim: 0.21410881621991132, 1.0000006379504391 split point = 0.6063373044130151,0.6739438231359857,0.7314970199457075,0.7777690591822977,0.8349141758721886
    //    tfidf: 0.2383919317923733, 15.95276488961107 split point = 1.1868150751606872,1.5173827744662627,1.8774999893992907,2.3586005125563525,3.173667095271042
    println("size: " + size)
    println("tit_sim: " + tit_sim.min + ", " + tit_sim.max + " split point = " + tit_sim_point.mkString(",")) //0.6 0.67 0.73 0.78 0.83
    println("tfidf: " + tfidf.min + ", " + tfidf.max + " split point = " + tfidf_point.mkString(",")) //1.2 1.5 1.9 2.4 3.2
    (tit_sim_point.toArray, tfidf_point.toArray)
  }


  def training_lr(label_0: RDD[LabeledPoint], label_1: RDD[LabeledPoint]): Unit = {
    label_0.cache()
    label_1.cache()
    val size0 = label_0.count()
    val size1 = label_1.count()
    val size = Math.min(size0, size1).toInt
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "training***********************" + label_0.count() + "," + label_1.count() + "*****************************")
    val start = System.currentTimeMillis()
    val Array(trainingData0, testData0) = sc.parallelize(label_0.take(size)).randomSplit(Array(0.9, 0.1))
    val Array(trainingData1, testData1) = sc.parallelize(label_1.take(size)).randomSplit(Array(0.9, 0.1))
    val tra = trainingData0 ++ trainingData1
    val test = testData0 ++ testData1
    val lr = new LogisticRegressionWithLBFGS().setIntercept(true).setNumClasses(2)
    lr.optimizer.setRegParam(0.6).setConvergenceTol(0.000001).setNumIterations(5000)
    //.setUpdater(new SquaredL2Updater)
    val model = lr.run(tra).setThreshold(0.5)
    val scoreAndLabels = test.mapPartitions(iter => for (r <- iter) yield {
      val pre = model.predict(r.features)
      (pre, r.label)
    })
    for ((pre, label) <- scoreAndLabels.take(10)) {
      println("training " + pre + "," + label)
    }
    label_0.unpersist()
    label_1.unpersist()
    val TP = scoreAndLabels.filter(r => (r._1 >= 0.5 && r._2 == 1)).count()
    val FP = scoreAndLabels.filter(r => (r._1 >= 0.5 && r._2 == 0)).count()
    val FN = scoreAndLabels.filter(r => (r._1 < 0.5 && r._2 == 1)).count()
    val TN = scoreAndLabels.filter(r => (r._1 < 0.5 && r._2 == 0)).count()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "training***********************TP = " + TP + ", FP = " + FP + ", FN = " + FN + ", TN = " + TN + "*****************************")
    if (TP + FP != 0) {
      val p = TP.toDouble / (TP + FP)
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "training***********************p = " + p + "*****************************")
    }
    if (TP + FN != 0) {
      val r = TP.toDouble / (TP + FN)
      println(sdf_time.format(new Date((System.currentTimeMillis()))) + "training***********************r = " + r + "*****************************")
    }


    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    var auc = metrics.areaUnderROC()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "training***********************auc = " + auc + "*****************************")
    val end = System.currentTimeMillis()
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "training***********************" + (end - start) / 1000 + " s *****************************")
    //    model
    val weight = model.weights.toArray.mkString(",")

    save(weight)
    println("weight:\n" + weight)
    println("model:\n" + model.toString())

  }

  def save(result: String): Unit = {
    val outputFile = "/tmp/xueyuan/kw_lr/weight.txt"
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val outputstream = hdfs.create(new Path(outputFile), true)

    outputstream.writeBytes(result)

    outputstream.close()

  }

  def tran(v: Double, array: Array[Double]) = {
    var index = array.length
    var flag = false
    for (i <- 0 until array.length if flag == false) {
      if (v < array(i)) {
        index = i
        flag = true
      }

    }
    var result = Array.fill(array.length + 1)(0.0)
    result(index) = 1.0
    result
  }


  def get_feature(id_kw_title_content_p1_cate: RDD[(Long, Array[String], Array[String], Array[String], Array[String], Array[Double])], word_idf_br: Broadcast[Map[String, Double]], word_vec_br: Broadcast[Map[String, Array[Float]]], label: Int) = {
    //    val word_vec_br = sc.broadcast(word_vec)
    //    val word_idf_br = sc.broadcast(word_idf)
    //label=1 时 返回空集
    if (label == 1) {
      println("label==1 id_kw_title_content_p1_cate_size=" + id_kw_title_content_p1_cate.count())
      for ((id, kw, tit, con, p1, cate) <- id_kw_title_content_p1_cate.take(10)) {
        println("kw=" + kw.mkString(",") + "; tit=" + tit.mkString(",") + "; con=" + con.mkString(","))
      }
      println("label==1 word_vec_br=" + word_vec_br.value.size)
      println("label==1 word_idf_br=" + word_idf_br.value.size)
    }
    val label_feature = id_kw_title_content_p1_cate.mapPartitions(iter => for (r <- iter) yield {
      val word_vec = word_vec_br.value
      val word_idf = word_idf_br.value
      val title = r._3
      //标题中所有词向量相加
      var tit_vec: Array[Float] = null
      if (title.length > 0) {
        for (t <- title) {
          val t_vec = word_vec(t)
          if (tit_vec == null) {
            tit_vec = t_vec
          } else {
            val tmp = tit_vec.zip(t_vec).map(rr => rr._1 + rr._2)
            tit_vec = tmp
          }
        }
      }
      val content = r._4
      val p1 = r._5
      val cate = r._6

      val total_word_size = (title ++ content).length
      var res = new ArrayBuffer[(Int, (Array[Double], Array[Double], Double, Double, Array[Double]))]()
      for (kw <- r._2) {
        val in_title = if (title.contains(kw) == true) Array(1.0, 0.0) else Array(0.0, 1.0)
        val in_p1 = if (p1.contains(kw) == true) Array(1.0, 0.0) else Array(0.0, 1.0)
        var tit_sim = 0.0
        if (tit_vec != null) {
          val kw_vec = word_vec(kw)
          tit_sim = (tit_vec.zip(kw_vec).map(rr => rr._1 * rr._2).reduce(_ + _)) / ((math.sqrt(tit_vec.map(rr => math.pow(rr, 2)).reduce(_ + _))) * (math.sqrt(kw_vec.map(rr => math.pow(rr, 2)).reduce(_ + _))))
        }
        var tf = 0.0
        if (total_word_size != 0) {
          tf = ((title ++ content).filter(_.equals(kw)).length).toDouble / total_word_size
        }

        val idf = word_idf(kw)

        res += ((label, (in_title, in_p1, tit_sim, tf * idf, cate)))
        println("label==1 res_size=" + res.length)
      }
      println("label==1 res_size_sum=" + res.length)
      res
    }).flatMap(r => r)
    if (label == 1) {
      println("label==1 label_feature=" + label_feature.count())
    }
    label_feature
  }

  def load_data() = {
    val sql = "select id,kw,title,content,p1,cate from algo.xueyuan_tit_p1_con_key_cate where stat_date=20170807"
    val df = hiveContext.sql(sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************load*****************************")
    val id_kw_title_content_p1_cate = df.map(r => (r.getLong(0), r.getString(1).split(","), r.getString(2).split(","), r.getString(3).split(","), r.getString(4).split(","), r.getString(5))).map(r => {
      //      var kw = new ArrayBuffer[String]()
      //      for (w <- r._2) {
      //        kw += w.trim
      //      }
      //      val content = r._3
      //      val i = content.indexOf("<p>")
      //      val j = content.indexOf("</p>")
      //      var p1 = ""
      //      if (i >= 0 && j >= 0 && i < j) {
      //        p1 = content.substring(i + 1, j)
      //      }

      val kw = r._2
      val tit = r._3
      val con = r._4
      val p1 = r._5
      val cate = r._6
      (r._1, kw, tit, con, p1, cate)
    })
    id_kw_title_content_p1_cate
  }


  def queryWordVec() = {
    //    val srcSql = "select word from algo.lc_article_word_tfidf where idf <=3 or tf > 165943 and tf is not null and idf is not null"
    //    val srcData = SparkApp.hiveContext.sql(srcSql).map(r => r.getString(0))
    //    val stopWord = srcData.collect().toSet

    Log.info("load word vec start")
    //    val stopWordBroad = SparkApp.sc.broadcast(stopWord)
    val srcSql_2 = s"select word,vec from algo.lj_article_word_vec2 where stat_date='20170804'"
    val srcData_2 = SparkApp.hiveContext.sql(srcSql_2)
      .map(r => {
        val vec = r.getString(1).split(",").map(_.toFloat)
        Blas.norm2(vec)
        (r.getString(0), vec)
      }) //.filter(r => !stopWord.contains(r._1))
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


}
