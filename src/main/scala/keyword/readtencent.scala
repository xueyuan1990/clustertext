/**
  * Created by xueyuan on 2017/7/31.读取腾讯的excel文章存入数据库
  */
package keyword

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.hadoop.fs.Path
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import utils.StrUtil

import scala.collection.mutable.ArrayBuffer


object readtencent {
  val sdf_time: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss")
  val sdf_date = new SimpleDateFormat("yyyyMMdd")
  var sc: SparkContext = null
  var hiveContext: HiveContext = null
  var file_path = "/tmp/xueyuan/tencent.xls"

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
    val kw_tit_url_con_array = readTable()
    for ((kw, tit, url, con) <- kw_tit_url_con_array.take(10)) {
      println(kw + " " + tit + " " + url + " " + con)
    }
    val data = sc.parallelize(kw_tit_url_con_array)

    save(data)
  }


  def readTable() = {
    val hdfs = org.apache.hadoop.fs.FileSystem.get(new org.apache.hadoop.conf.Configuration())
    val path1 = new Path(file_path)
    val a = hdfs.open(path1)
    //    val ips = new FileInputStream(file_path);
    val wb = new HSSFWorkbook(a);
    val sheet = wb.getSheetAt(0);

    var ite = sheet.rowIterator();
    var kw_tit_url_con_array = new ArrayBuffer[(String, String, String, String)]()
    while (ite.hasNext()) {
      val row = ite.next();
      var itet = row.cellIterator();
      var i = 0
      var temp = new Array[String](4)
      while (itet.hasNext()) {
        val cell = itet.next();
        //读取String
        val s = cell.getRichStringCellValue().toString
        temp(i) = stripHtml(s)
        i = i + 1
      }
      val kw_tit_url_con = (temp(0), temp(1), temp(2), temp(3))
      kw_tit_url_con_array += kw_tit_url_con
    }
    kw_tit_url_con_array.toArray
  }

  def stripHtml(content: String): String = {
    if (StrUtil.isEmpty(content)) ""
    else {
      content.replaceAll("\n", " ").replaceAll("<script>.*?</script>", "")
        .replaceAll("(</p>|</br>)\n*", "\n")
        .replaceAll("<[^>]+?>", " ")
        .replaceAll("(点击加载图片)|(查看原文)|(图片来源)", " ")
        .replaceAll("\\s*\n\\s*", "\n")
        .replaceAll("[ \t]+", " ")
        .replaceAll("\n", " ")
    }
  }

  def save(data: RDD[(String, String, String, String)]): Unit = {
    val candidate_rdd = data.map(r => Row(r._1, r._2, r._3, r._4))

    val structType = StructType(
      StructField("keyword", StringType, false) ::
        StructField("title", StringType, false) ::
        StructField("url", StringType, false) ::
        StructField("article", StringType, false) ::
        Nil
    )
    //from RDD to DataFrame
    val table_out = "algo.xueyuan_article_key_tencent"
    val candidate_df = hiveContext.createDataFrame(candidate_rdd, structType)
    val create_table_sql: String = "create table if not exists " + table_out + " (keyword String,title String, url String, article string  )  stored as textfile"
    val c1 = Calendar.getInstance()
    //        c1.add(Calendar.DATE, -1)
    val sdf1 = new SimpleDateFormat("yyyyMMdd")
    val date1 = sdf1.format(c1.getTime())
    val insertInto_table_sql: String = "insert overwrite table " + table_out + " select * from "
    val table_temp = "table_temp"
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************save data start*****************************")
    candidate_df.registerTempTable(table_temp)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************register TempTable finished*****************************")
    hiveContext.sql(create_table_sql)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************create table finished*****************************")
    hiveContext.sql(insertInto_table_sql + table_temp)
    println(sdf_time.format(new Date((System.currentTimeMillis()))) + "***********************insertInto table finished*****************************")
  }
}
