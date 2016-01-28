package com.caishi.kmeans

import com.caishi.model.Util
import org.apache.hadoop.fs._
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 根据用户画像聚合相同用户并存储hdfs
  * Created by YMY on 16-1-26.
  */
object UserKMeans {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: UserKMeans <hdfsUrl> <fromDir> <toDir> <numCenter> <numIterations>")
      System.exit(1)
    }
    val Array(hdfsUrl,fromDir, toDir, numCenter,numIterations) = args

//    val hdfsUrl ="hdfs://10.4.1.4:9000"
//    val fromDir ="hdfs://10.4.1.4:9000/hivedata/profiles/user_catLike.json"
//    val toDir ="hdfs://10.4.1.4:9000/hivedata/profiles/user_group"
//    val numCenter = 11
//    val numIterations = 10
    val conf = new SparkConf()

    conf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)

    //装载数据
    val data = sc.textFile(fromDir.toString)
    val cachedData = data.map(line => Util.jsonToObject(line)).cache()
    val parsedData = cachedData.map(p => {
      val x = p.values().toArray()(0).toString.replace("[","").replace("]","").split(",").map(_.toDouble)
      Vectors.dense(x)
    })
    val model = KMeans.train(parsedData,numCenter.toInt, numIterations.toInt)
    val userGroups = cachedData.map(item => {
      item.keySet().iterator().next()+","+model.predict(Vectors.dense(item.values().toArray()(0).toString.replace("[","").replace("]","").split(",").map(_.toDouble)))+","+System.currentTimeMillis()
    })

    sc.hadoopConfiguration.set("fs.defaultFS",hdfsUrl)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(toDir),true)
    userGroups.saveAsTextFile(toDir)

  }


  /** 删除原始小文件 */
  def del(toDir : String,fs : FileSystem): Unit ={

    val files : Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(toDir)))
    for(f : Path <- files){
      fs.delete(f,true)// 迭代删除文件或目录
    }
  }
}
