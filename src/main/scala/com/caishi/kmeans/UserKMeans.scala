package com.caishi.kmeans

import com.alibaba.fastjson.JSON
import com.caishi.model.{RedisUtil, Util}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.hadoop.fs._
import org.apache.log4j.Logger
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import redis.clients.jedis.JedisPool

/**
  * 根据用户画像聚合相同用户并存储hdfs
  * Created by YMY on 16-1-26.
  */
case class News(userId:String,newsId :String,newsType : String )
case class UG(userId:String,center:Int)
object UserKMeans {
  val logger = Logger.getLogger("UserKMeans")
  def main(args: Array[String]) {
    if (args.length < 10) {
      logger.error("Usage: UserKMeans <hdfsUrl> <fromDir> <toDir> <numCenter> <numIterations>")
      System.exit(1)
    }
    val Array(appName,hdfsUrl,userProfilesDir,common_eventDir, toDir, numCenter,numIterations,redisHost,redisAuth,dataKey) = args

//    val appName = "test"
//    val hdfsUrl ="hdfs://host:9000"
//    val userProfilesDir ="hdfs://host:9000/hivedata/profiles/user_catLike.json.bak"
//    val toDir ="hdfs://host:9000/hivedata/profiles/user_group"
//    val common_eventDir = "hdfs://host:9000/logdata/2016/04/13/topic_common_event/18"
//    val numCenter = 100
//    val numIterations = 10
//
//    val redisHost = "host:6380"
//    val redisAuth = "xxx"

//    val redisHost = "host:6385"
//    val redisAuth = "cs"

//    val dataKey="UserCF"

    val conf = new SparkConf().setAppName(appName)

    conf.set("spark.driver.allowMultipleContexts","true")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS",hdfsUrl)
    //装载数据
    val data = sc.textFile(userProfilesDir.toString)
    // 读取用户画像数据并平铺200个列
    val cachedData = data.map(line => Util.jsonToObject(line)).cache()
    val parsedData = cachedData.map(p => {
      val x = p.values().toArray()(0).toString.replace("[","").replace("]","").split(",").map(_.toDouble)
      Vectors.dense(x)
    })
    val model = KMeans.train(parsedData,numCenter.toInt, numIterations.toInt)
    // 根据model对用户进行分组
    val userGroups = cachedData.map(item => {
      item.keySet().iterator().next()+","+model.predict(Vectors.dense(item.values().toArray()(0).toString.replace("[","").replace("]","").split(",").map(_.toDouble)))+","+System.currentTimeMillis()
    })
    //
    val sqlContext = new SQLContext(sc)
    // 加载用户浏览新闻条目数据
    val news = sqlContext.read.parquet(common_eventDir)
    val par = news.select(news("userId"),news("data")).map(row => (row.get(0), JSON.parseObject(row.get(1).toString).getString("param")))
    import sqlContext.implicits._
    val newsTable = par.filter(_ != null).filter(_._2 != null).filter(_._2.contains("newsType")).map(param => {
      val p = JSON.parseObject(param._2)
      News(String.valueOf(param._1),String.valueOf(p.get("newsId")),String.valueOf(p.get("newsType")))
    }).toDF()
    // 注册用户|新闻|新闻类型 table
    newsTable.registerTempTable("newsTable")
    // 将用户分组后的数据注册为临时表
    val ug = userGroups.map(item => item.split(",")).map(p => UG(p(0),p(1).toInt)).toDF()
    ug.registerTempTable("usergroup")
//    var center = "";
//    val x = userGroups.filter(_.split(",")(0)=="cae3572873941b09a2fee007c26b79d9").take(1).map((item)=>{center=item.split(",")(1); 1})
//    sqlContext.sql("select * from usergroup where center='"+center+"'").show(1000,false)

    // 将用户浏览记录和用户组进行联合查询，并注册新表（spark sql目前不支持嵌套查询）
    val center_news_table = sqlContext.sql("select nt.newsId as newsId, ug.center as center,count(1) as callcount,nt.newsType as newsType from newsTable as nt left join usergroup as ug on ug.userId=nt.userId group by center,newsId,newsType")
    center_news_table.registerTempTable("center_news_table")
    val user_center_news_table = sqlContext.sql("select ug.userId,cnt.* from usergroup as ug left outer join center_news_table as cnt on ug.center=cnt.center")
    user_center_news_table.registerTempTable("user_center_news_table")
    val result = sqlContext.sql("select ucnt.*, nt.* from user_center_news_table as ucnt left join newsTable as nt on ucnt.userId=nt.userId and ucnt.newsId=nt.newsId where nt.newsId is null ")


    //    val joinTable = newsTable.where(newsTable("newsType")==="NEWS").join(ug.select(ug("userId"),ug("center")),newsTable("userId")=== ug("newsId"),"left")
//    center_news_table.show(20,false)
//    user_center_news_table.show(200,false)
//    result.show(100,false)
    result.map(row => row.get(0)->row.get(1)).reduceByKey(_+","+_).foreachPartition(items =>{
//      val pool = createRedisPool(redisHost,redisPort.toInt,redisAuth)
      val jedis = RedisUtil.getRedisPool(redisHost,redisAuth).getResource
//      jedis.select(redisDBIndex.toInt)
      items.foreach(item =>{
        // jedis累加数值
        //        jedis.hincrBy(clickHashKey, uid, clickCount)
        jedis.set(dataKey+"_"+item._1.toString,item._2.toString)
        // 设置过期时间
        jedis.expire(dataKey+"_"+item._1.toString,1*60*60)
      })
      jedis.close()
//      pool.returnResourceObject(jedis)
    })

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

  // 获得redis连接池
  def createRedisPool(host: String, port: Int, pwd: String): JedisPool = {
    val pc = new GenericObjectPoolConfig()
    pc.setMaxIdle(200)
    pc.setMaxTotal(200)
    pc.setMinIdle(20)
    pc.setMaxWaitMillis(5*1000)

    new JedisPool()
    new JedisPool(pc, host, port, 10000, pwd)
  }
}
