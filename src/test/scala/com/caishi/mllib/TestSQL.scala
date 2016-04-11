package com.caishi.mllib

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by YMY on 16-2-24.
  */
case class News(userId:String,newsId :String,newsType : String )
object TestSQL {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("test")

    conf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read.parquet("hdfs://10.4.1.4:9000/logdata/2016/02/24/topic_common_event/09")
    df.printSchema()
    val x = df.select(df("userId"),df("data")).map(row => (row.get(0), JSON.parseObject(row.get(1).toString).getString("param")))

    import sqlContext.implicits._
    x.filter(_._2.contains("newsType")).map(param => {
      val p = JSON.parseObject(param._2)
      News(param._1.toString,p.get("newsId").toString,p.get("newsType").toString)
    }).toDF().show(50,false)
//    case class Person(name: String, age: Int)
//    val people = sc.textFile("examples/src/main/resources/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
//    people.registerTempTable("people")
  }
}
