package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by linjiang on 2017/4/19.
  */
object SparkApp {
  val conf = new SparkConf().setAppName("news cluster")
  val sc = SparkContext.getOrCreate(conf)
  sc.hadoopConfiguration.set("mapred.output.compress", "false")
  val hiveContext = new HiveContext(sc)
  val sqlContext = new SQLContext(sc)

  def saveToHive(pd: RDD[Row], saveTable: String, schema: StructType): Unit = {
    Log.info(s"begin save to hive table = $saveTable")

    val hiveContext = SparkApp.hiveContext
    val newDF = hiveContext.createDataFrame(pd, schema)

    val tmp_table = "lj_tmp_table"
    newDF.registerTempTable(tmp_table)

    val insertSql = s"INSERT OVERWRITE TABLE $saveTable SELECT * FROM $tmp_table"
    hiveContext.sql(insertSql)
    hiveContext.dropTempTable(tmp_table)

    Log.info(s"finish save to hive table = $saveTable")
  }

  def saveToHive(pd: RDD[Row], saveTable: String, schema: StructType, stat_date: String,statIsString:Boolean=true): Unit = {
    Log.info(s"begin save to hive table = $saveTable")

    val hiveContext = SparkApp.hiveContext
    val newDF = hiveContext.createDataFrame(pd, schema)

    val tmp_table = "lj_tmp_table"
    newDF.registerTempTable(tmp_table)

    val st=if (statIsString) s"'$stat_date'" else stat_date

    val insertSql = s"INSERT OVERWRITE TABLE $saveTable PARTITION(stat_date=$st ) SELECT * FROM $tmp_table"
    hiveContext.sql(insertSql)
    hiveContext.dropTempTable(tmp_table)

    Log.info(s"finish save to hive table = $saveTable")
  }
}
