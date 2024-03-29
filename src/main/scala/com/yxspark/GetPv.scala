package com.yxspark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}
import java.io.File
import java.text.SimpleDateFormat


case class SadaRecord(scrip:String, ad:String, ts:String, url:String, ref:String, ua:String, dstip:String,cookie:String,
                      srcPort:String)
object GetPv {
  val spark = SparkSession.builder().appName("GetPv").getOrCreate()
  val sc = spark.sparkContext
  val todayStr = new SimpleDateFormat("yyyyMMdd").format(new java.util.Date())
  val timeStr = new SimpleDateFormat("HHmmss").format(new java.util.Date())

  val fs = FileSystem.get(sc.hadoopConfiguration)

  import spark.implicits._

  def varsMap = Map(("delm","\t"),("underscore","_"),("ad","ad"),("total","_total"))

  private  def tagStringSuf(prjName:String)= prjName match {
      case "daoxila" => ":96928575"
      case "futures" => ":96928579"
      case _ => ""
  }


  private def createDir(path:String): Unit ={
    val pathDir = new Path(path).getParent

    if (!fs.exists(pathDir)){
      println(s"destination directory ${pathDir} not exists, creating ")
      fs.mkdirs(pathDir)
    }
  }
  private def deleteFile(fileName:String, prefix:String = ""): Unit ={
    if(fs.exists(new Path(fileName))){
      println(s"hadoop ${prefix} file path: ${fileName} already exsits, deleting...")
      fs.delete(new Path(fileName), true)
    }else if(new File(fileName).exists()){
      println(s"local file ${prefix} file path: ${fileName} already exsits, deleting...")
      new File(fileName).delete()
    }
  }


  def stg_s0(adcookiePath:String, newclickPath:String, postPath:String, urlPath:String, savePath:String): Unit = {
    //println(s"source files: ${adcookiePath} ${newclickPath} ${postPath}")
    val sadaRecordArr = Array("scrip", "ad", "ts", "url", "ref", "ua", "dstip","cookie", "srcPort")

    val data = sc.textFile("%s,%s".format(adcookiePath,newclickPath))
//    val data = sc.textFile("%s,%s,%s".format(adcookiePath,newclickPath,postPath))
    val urls = sc.textFile(urlPath).map(l => l.split(" +")).collect().toList

    val sourceDS = data.map{
      line =>
        val arr:Array[String] = line.split("\t")
        val ref = if (arr(4).toLowerCase == "nodef") "" else arr(4)
        val ua = if ( arr(5).toLowerCase  == "nodef") "" else arr(5)
        val cookie = if (arr(7).toLowerCase == "nodef") "" else arr(7)
        (arr(0),arr(1),arr(2),arr(3),ref,ua,arr(6),cookie,arr(8))
    }.toDF(sadaRecordArr:_*).
        withColumn("url", regexp_replace($"url","\t","")).
        withColumn("ref",regexp_replace(decode(unbase64($"ref"),"UTF-8"),"\t","")).
        //withColumn("ua",decode(unbase64($"ua"),"UTF-8")).
        withColumn("cookie",regexp_replace(decode(unbase64($"cookie"),"UTF-8"),"\\p{C}","?")).as[SadaRecord]


    val saveTbl = sourceDS.filter{
      record =>
        urls.exists(l => l.forall(record.url.contains(_)))
    }.toDF(sadaRecordArr:_*)
    //println("src data count"+ dataFilter.count)
    saveTbl.coalesce(1000).write.format("com.databricks.spark.csv").
      option("delimiter", varsMap("delm"))
      .save(savePath)
  }

  // filter data based ts col -- unxi epoch timestamp
  //case class Record(srcip:String, ad:String, ts:Long, url:String, ref:String, desip:String, cookie:String, src_port: String, tslag:Long);
  def filterData(srcpath:String, destpath:String,destMatchPath:String, validinterval:Int = 300, lowlimit:Int = 300, uplimit:Int= 10800): Unit ={
    // validinterval -- valid time interval in seconds
    // lowlimit -- min stay time in seconds
    // uplimit -- max stay time in seconds
    //println(s"filter data : source file ${srcpath}")
    //println(s"destination file: ${destpath}")
    //println(s"valid interval : ${validinterval}s, low limit ${lowlimit}, uplimit: ${uplimit}")
    //val columns = Array("srcip","ad","ts","url","ref","ua","desip","cookie","src_port")
    val delm = varsMap("delm")
    val srcData = sc.textFile(srcpath).map{
      row =>
        val arr = row.split(delm)
        (arr(0), arr(1),arr(2).toLong, arr(3).split("//")(1).split("/")(0), arr(4), arr(5), arr(6), arr(7), arr(8))
    }.toDF("srcip","ad","ts","url","ref","ua","desip","cookie","src_port")

    val wSpec = Window.partitionBy("ad","ua").orderBy($"ts")
    val dataSort = srcData.withColumn("tslag", lag("ts", 1, 0).over(wSpec)).filter($"tslag" =!= 0)

    val dataValid = dataSort.withColumn("interval", ($"ts" - $"tslag")/1000).filter($"interval" < validinterval).
      groupBy("ad","ua","url").agg(sum($"interval") as "totaltime", max("srcip") as "srcip", max("ref") as "ref",
      max("desip") as "desip", max("cookie") as "cookie", max("src_port") as "src_port")

    val dataFilter = dataValid.filter($"totaltime" >= lowlimit && $"totaltime" <= uplimit).select("srcip",
      "ad","totaltime","url","ref","ua","desip","cookie","src_port")
    val dataFilterMatch = dataFilter.select("ad","ua","url")//.filter("ad != '' and ad != 'none'")
    dataFilterMatch.write.format("com.databricks.spark.csv").option("delimiter",delm).save(destMatchPath)
    //dataFilter.write.format("com.databricks.spark.csv").option("delimiter","\t").save(destpath)
    //println("after filter, data count: " + dataFilter.count)
  }

  def filterDataNew(srcpath:String, destPath:String, destMatchPath:String, paraFile:String): Unit ={
    val delm = varsMap("delm")
    val srcData = sc.textFile(srcpath).map{
      row =>
        val arr = row.split(delm)
        (arr(0), arr(1),arr(2).toLong, arr(3).split("//")(1).split("/")(0), arr(4), arr(5), arr(6), arr(7), arr(8))
    }.toDF("srcip","ad","ts","url","ref","ua","desip","cookie","src_port")
    val params = sc.textFile(paraFile).map(l => l.split(" +")).collect()
  }


  //case class Record(mobile:String, url:String)
  def matchPortal(varsmap:Map[String,String], filterPath:String,  matchSaveFile:String,af:Int,pieceAmount:Int): Unit ={

    val delm = varsmap("delm")
    val data = sc.textFile(filterPath).map{
      row =>
        val arr =row.split(delm)
        (arr(0),arr(1),arr(2))
    }
    val counts:Int = math.ceil(data.count()/pieceAmount.toFloat).toInt
    import hlwbbigdata.phone

    val dataPieces =data.randomSplit(Array.fill(counts)(1))
    for  ((piece,idx) <- dataPieces.zipWithIndex){
      val matchResult = phone.phone_match(spark,piece, af.toString)
      matchResult.write.format("com.databricks.spark.csv").option("delimiter", delm).save(matchSaveFile+ "_" + idx)

    }

  }

  def combinMatch(matchSaveFile:String): Unit ={
    val res = sc.textFile(matchSaveFile+"_*")
    res.saveAsTextFile(matchSaveFile)
  }

  def dropHistory(prjName:String,tagName:String,tagFile:String, mathePath:String,historyPath:String,outPath:String): Unit ={
    val delm = varsMap("delm")
    val tagNames =sc.textFile(tagFile).map(l => l.split(" +")).collect()
    val inDF = sc.textFile(mathePath).map(row => (row.split(delm)(0), row.split(delm)(1))).map{
      row =>
        val appName = tagNames.filter(l => row._2.contains(l(0)) || l(0) == "*").
          map(l => l(1)).headOption.getOrElse("")
        (row._1, appName)
    }.toDF("mobile","url").filter("url != ''").dropDuplicates()

    val tagString = ":" + tagName + "_" + todayStr  + tagStringSuf(prjName)

    val hisDF = sc.textFile(historyPath).map(row => row.split(delm)(0)).toDF("mobile")
    val tagDF = inDF.join(hisDF,Seq("mobile"),"leftanti").withColumn("tag", concat($"url",lit(tagString))).drop($"url").dropDuplicates(Seq("mobile"))

    tagDF.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(outPath)

  }

  def kvTag( key:String, inPath:String, kvPath :String, outHistoryPath:String): Unit ={
    val delm = varsMap("delm")
    val underScore = varsMap("underscore")
    val ad  = varsMap("ad")
    val total = varsMap("total")
//    val inDF = sc.textFile(inPath).map{
//      row =>
//        val arr = row.split(delm,2)
//        (arr(0),arr(1))
//    }.zipWithIndex().map(
//      row =>
//        (key + underScore + todayStr + underScore + row._2, ad + delm + row._1._2 + delm + row._1._1)
//    ).toDF("key","value")

    val inDF = sc.textFile(inPath).map{
      row =>
        val arr = row.split(delm,2)
        (arr(0),arr(1))
    }.toDF("key","value").
      select(concat(lit(key + underScore + todayStr + underScore),row_number().over(Window.orderBy("value")))
          , concat(lit(ad + delm),$"value" ,lit(delm), $"key")).toDF("key","value")

    val totalLine = (key + underScore + todayStr+ total, inDF.count.toString )
    val counts = Seq(totalLine).toDF("key","value")

    val kvTbl = inDF.union(counts)
    val historyDF = inDF.select("value").map(row => row.getString(0).split(delm)(2))
    kvTbl.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(kvPath)
    historyDF.coalesce(1).write.format("com.databricks.spark.csv").option("delimiter",delm).save(outHistoryPath)

  }


  // arg 0 : project name must supply.
  def main(args: Array[String]): Unit = {
    assert(args.length >= 1, "project name must supply")
    val prjName = args(0)
    val dateStr = if (args.length < 2) new SimpleDateFormat("yyyyMMdd").format(new java.util.Date()) else args(1)
    val seconds = args(2).toInt
    val tagName = args(3)
    val arg4 = args.lift(4).getOrElse("10000")
    val runAll = if (arg4 == "all") true else false

    val publicPath = "hdfs://ns1/user/gdpi/public"
    val addcookiePath = s"${publicPath}/sada_gdpi_adcookie/${dateStr}/*/*.gz"
    val newclickPath = s"${publicPath}/sada_new_click/${dateStr}/*/*.gz"
    val postPath = ""
//    val postPath = s"${publicPath}/sada_gdpi_post_click/${dateStr}/*/*.gz"

    // configure file path
    val urlPath = s"hdfs://ns1/user/u_tel_hlwb_mqj/private/pv/pvconfig/${prjName}_url.txt"
    val tagPath = s"hdfs://ns1/user/u_tel_hlwb_mqj/private/pv/pvconfig/${prjName}_appname.txt"
    // private path
    val privatePath = s"hdfs://ns1/user/u_tel_hlwb_mqj/private/pv/${prjName}/${dateStr}"
    val savePath = s"${privatePath}/${prjName}_pv_${dateStr}"
    val filterPath = s"${privatePath}/${prjName}_pv_filter_${dateStr}"
    val matchFilterPath = s"${privatePath}/${prjName}_pv_match_filter_${dateStr}"
    val matchPortalPath = s"${privatePath}/${prjName}_pv_match_portal_${dateStr}"
    val dropHistoryPath = s"${privatePath}/${prjName}_pv_drop_history_${dateStr}"
    val kvPath = s"${privatePath}/${prjName}_pv_kv_${dateStr}"

    // history file path
    val historyPath = s"hdfs://ns1/user/u_tel_hlwb_mqj/private/lxc_xgq/${prjName}_final_history/*"
    val saveHistoryPath = s"hdfs://ns1/user/u_tel_hlwb_mqj/private/lxc_xgq/${prjName}_final_history/${prjName}_pv_${dateStr}"

    // test file exists or not
    //assert(new File(urlPath).exists, s"url file ${urlPath} not exist, please check again")
    //assert(new File(tagPath).exists(),s"tag file ${tagPath} not exist, pease check again")
    //createDir(savePath)
//    // delete file
//    deleteFile(filterPath)
//    deleteFile(matchFilterPath)
//    deleteFile(matchPortalPath)
//    deleteFile(dropHistoryPath)
//    deleteFile(kvPath)
//    deleteFile(saveHistoryPath)
    // get relate url from source

    val pieceAmount =  if (runAll){
      stg_s0(addcookiePath, newclickPath, postPath, urlPath, savePath)
      args.lift(5).getOrElse("10000").toInt
    }else{
      arg4.toInt
    }

    filterData(savePath,filterPath,matchFilterPath,seconds)
    matchPortal(varsMap,matchFilterPath,matchPortalPath,0,pieceAmount)
    combinMatch(matchPortalPath)
//    dropHistory(prjName,tagName,tagPath,matchPortalPath,historyPath,dropHistoryPath)
//    kvTag(tagName,dropHistoryPath,kvPath,saveHistoryPath)
  }

}
