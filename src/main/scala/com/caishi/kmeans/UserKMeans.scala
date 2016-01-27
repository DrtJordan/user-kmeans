package com.caishi.kmeans

import com.caishi.model.Util
import com.mongodb.client.model.{UpdateOptions, Filters}
import com.mongodb.client.result.UpdateResult
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by YMY on 16-1-26.
  */
object UserKMeans {
  def main(args: Array[String]) {
//    if (args.length < 7) {
//      System.err.println("Usage: GeoKMeans <hdfsDirs> <numCenter> <numIterations> <position:home or office> <mongoRemotes:ip:port,ip:port> <mongoDb> <collection>")
//      System.exit(1)
//    }
//    val Array(hdfsDirs, numCenter, numIterations, position,mongoRemotes, mongoDb,collection) = args

        val hdfsDirs ="hdfs://10.4.1.4:9000/test/newbak.json"
        val numCenter = 11
        val numIterations = 10
    //    val position="office"
    //    val mongoRemotes="10.1.1.134:27017"
    //    val mongoDb = "mydb"
    //    val collection ="test"
    //    val conf = new SparkConf().setAppName("mllib-geo").setMaster("local[2]")
    val conf = new SparkConf().setAppName("mllib-geo").setMaster("local")

    conf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)

    //装载数据
    val data = sc.textFile(hdfsDirs.toString)
    val cachedData = data.map(line => Util.jsonToObject(line)).cache()
    val parsedData = cachedData.map(p => {
      val x = p.values().toArray()(0).toString.replace("[","").replace("]","").split(",").map(_.toDouble)
//      println(p.keySet().iterator().next()+"   "+x.size)
      Vectors.dense(x)
    })
    val model = KMeans.train(parsedData,numCenter, numIterations)
    val userGroups = cachedData.map(item => {
      item.keySet().iterator().next()+","+model.predict(Vectors.dense(item.values().toArray()(0).toString.replace("[","").replace("]","").split(",").map(_.toDouble)))
    })
    userGroups.saveAsTextFile("file:///opt/work/test/")
  }
}
