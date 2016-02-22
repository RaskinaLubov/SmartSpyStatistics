
import org.apache.spark.sql.{Row, SQLContext, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.{Period, DateTime}
import org.joda.time.format.PeriodFormatterBuilder
import org.slf4j.LoggerFactory


/**
  * Created by Raslu on 13.02.2016.
  */
object SmartSpyFunctions {
  def writeCommonStatistics(primaryStatDF : DataFrame): Unit ={
    import java.io._
    val writer = new PrintWriter(new File("C:\\Users\\Raslu\\IdeaProjects\\SmartSpyStatistics\\src/statistics.txt"))
    writer.write(PrintDF.showString(primaryStatDF.describe("counter","received","linkFaults","restored"
      ,"overflow","underflow","uptime","vidDecodeErrors","vidDataErrors"
      ,"avTimeSkew","avPeriodSkew","bufUnderruns","bufOverruns","dvbLevel"
      ,"curBitrate")
    )
    )

    writer.write(PrintDF.showString(primaryStatDF.filter("lost>=0").describe("lost")))
    writer.write(PrintDF.showString(primaryStatDF.filter("mdiDf>=0").describe("mdiDf")))
    writer.write(PrintDF.showString(primaryStatDF.filter("mdiMlr>=0").describe("mdiMlr")))
    writer.write(PrintDF.showString(primaryStatDF.filter("regionId>=0").describe("regionId")))
    writer.write(PrintDF.showString(primaryStatDF.filter("contentType>=0").describe("contentType")))

    writer.write(PrintDF.showString(primaryStatDF.filter("transportOuter>=0").describe("transportOuter")))
    writer.write(PrintDF.showString(primaryStatDF.filter("transportInner>=0").describe("transportInner")))
    writer.write(PrintDF.showString(primaryStatDF.filter("channelId>=0").describe("channelId")))
    writer.write(PrintDF.showString(primaryStatDF.filter("playSession>=0").describe("playSession")))
    writer.write(PrintDF.showString(primaryStatDF.filter("scrambled>=0").describe("scrambled")))
    writer.write(PrintDF.showString(primaryStatDF.filter("powerState>=0").describe("powerState")))
    //в инструкции написано 0 - неизвестно. Но в выборке присутствуют только значения -1(3) и 0 (30003)
    writer.write(PrintDF.showString(primaryStatDF.filter("casType>0").describe("casType"))) //primaryStatDF.groupBy("casType").count().show()
    //36 CAS_KEY_TIME  0 - неизвестно
    writer.write(PrintDF.showString(primaryStatDF.filter("casKeyTime>0").describe("casKeyTime")))//primaryStatDF.groupBy("casKeyTime").count().show()
    //37 VID_FRAMES 0 - видеостати стика недоступна
    writer.write(PrintDF.showString(primaryStatDF.filter("vidFrames>0").describe("vidFrames")))
    writer.write(PrintDF.showString(primaryStatDF.filter("audFrames>0").describe("audFrames")))
    //т.к. поле audDataErrors показывает наличие ошибок при даступной аудиостатискики (audFrames>0) 0 здесь тоже информация для записей которые audFrames>0
    writer.write(PrintDF.showString(primaryStatDF.filter("audFrames>0").describe("audDataErrors")))
    writer.write(PrintDF.showString(primaryStatDF.filter("sdpObjectId>=0").describe("sdpObjectId")))
    writer.write(PrintDF.showString(primaryStatDF.filter("dvbLevelGood>=0").describe("dvbLevelGood")))
    writer.write(PrintDF.showString(primaryStatDF.filter("dvbLevel>=0").describe("dvbLevel")))
    writer.write(PrintDF.showString(primaryStatDF.filter("dvbFrequency>=0").describe("dvbFrequency")))



    writer.write(PrintDF.showString(primaryStatDF.groupBy("msgType").count()))
    writer.write(PrintDF.showString(primaryStatDF.groupBy("streamType").count()))
    //primaryStatDF.groupBy("mac").count().show()
    //primaryStatDF.groupBy("streamAddr").count().show()
    writer.write(PrintDF.showString(primaryStatDF.groupBy("lostOverflow").count()))
    writer.write(PrintDF.showString(primaryStatDF.groupBy("plc").count()))
    //serviceAccountNumber
    //val dfFilterServiceAccountNumber = primaryStatDF.filter("serviceAccountNumber not in ('-1','N/A')")
    //dfFilterServiceAccountNumber.groupBy("serviceAccountNumber").count().join(dfFilterServiceAccountNumber.agg(count("serviceAccountNumber").as("countAll"))).show
    //primaryStatDF.groupBy("stbIp").count().show()
    writer.write(PrintDF.showString(primaryStatDF.groupBy("spyVersion").count()))
    //playerUrl
    val dfFilterPlayerUrl = primaryStatDF.filter("playerUrl not in ('X')")
    writer.write(PrintDF.showString(dfFilterPlayerUrl.groupBy("playerUrl").count().join(dfFilterPlayerUrl.agg(count("playerUrl").as("countAll")))))

    writer.close()

    def logger = LoggerFactory.getLogger(this.getClass)
    logger.info("select data about 5 users")

    val macListDF = primaryStatDF.groupBy(col("mac").as("mac1")).count().orderBy(desc("count")).limit(10).select("mac1")
    val macDF = primaryStatDF.join(macListDF,macListDF("mac1") === primaryStatDF("mac")).select(primaryStatDF.col("*"))//.select(primaryStatDF.columns.mkString(", "))
    //macDF.show
    //macDF.write.parquet("parquetTest")
  }

  def initN(sc: SparkContext, sqlContext: SQLContext, primaryStatDF: DataFrame, columnStat: Array[String]): DataFrame = {

    //create dfN
    val schemaN = StructType(
      StructField("mac", StringType, false) ::
        StructField("value", StringType, false) ::
        StructField("count", IntegerType, false) ::
        StructField("columnName", StringType, false) :: Nil)
    var dfN = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaN)

    for (i <- 0 to columnStat.size - 1) {
      val value = primaryStatDF.groupBy(col("mac"), col(columnStat(i)).as("value")).count().withColumn("columnName", lit(columnStat(i): String).cast(StringType))
      dfN = dfN.unionAll(value)
    }
    ///dfN.show(100)
    //end --create dfN
    return dfN
  }

  def initQ(sc: SparkContext, sqlContext: SQLContext, primaryStatDF: DataFrame, countCluster: Int): DataFrame = {
    //create dfQ
    val schemaQ = StructType(
      StructField("mac", StringType, false) ::
        StructField("cluster", IntegerType, false) ::
        StructField("pvod", DoubleType, false) :: Nil)
    var dfQ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaQ)

    /*
    dfQ.show(100)
    val valueQ = primaryStatDF.select("mac").distinct().withColumn("cluster", lit(1: Int).cast(IntegerType))
      .withColumn("pvod", rand().cast(DoubleType))
      .withColumn("pvod1", rand().cast(DoubleType))
    valueQ.show(100)
    valueQ.show(100)
    dfQ = dfQ.unionAll(valueQ)
    //dfQ.show(100)
    dfQ = dfQ.unionAll(valueQ).unionAll(valueQ)//если сделать два раза union одного и того же DF у одного и того же mac добавляются разные значения pvod!!!!
    dfQ.show(100)
    */

    val macDist = primaryStatDF.select(col("mac").as("macDist")).distinct()
    //macDist.show
    for (i <- 1 to countCluster) {
      val valueSum = macDist
        .join(dfQ, macDist("macDist") === dfQ("mac"), "left_outer")
        .groupBy(col("macDist")).agg(sum("pvod").as("sum_pvod"))
        .withColumn("cluster", lit(i: Int).cast(IntegerType))
        .withColumn("rand", rand().cast(DoubleType))
        .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
        .select(col("macDist").as("mac"), col("cluster"), col("pvod"))
      dfQ = dfQ.unionAll(valueSum)
    }
    //dfQ.show(100)
    val checksumCount = dfQ.groupBy(col("mac")).agg(sum("pvod").as("checksum")).filter("checksum<>1").count()
    if (checksumCount != 0) throw new Exception("checksumQCount != 1")
    //end --create dfQ
    //dfQ.show(100)
    return dfQ
  }

  def initJ(sc: SparkContext, sqlContext: SQLContext, countCluster: Int, dfN: DataFrame): Unit = {
    //create dfJ
    val schemaJ = StructType(
      StructField("cluster", IntegerType, false) ::
        StructField("columnName", StringType, false) ::
        StructField("value", StringType, false) ::
        StructField("pvod", DoubleType, false) :: Nil)
    var dfJ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaJ)

    val columnNameValueDist = dfN.select(col("columnName").as("columnName1"), col("value").as("value1")).distinct()
    for (i <- 1 to countCluster) {
      val valueJ = columnNameValueDist
        .join(dfJ, columnNameValueDist("columnName1") === dfJ("columnName") && columnNameValueDist("value1") === dfJ("value"), "left_outer")
        .groupBy(col("columnName1"), col("value1")).agg(sum("pvod").as("sum_pvod"))
        .withColumn("cluster", lit(i: Int).cast(IntegerType))
        .withColumn("rand", rand().cast(DoubleType))
        .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
        .select(col("columnName1").as("columnName"), col("value1").as("value"), col("cluster"), col("pvod"))
      //valueJ.show(100)
      dfJ = dfJ.unionAll(valueJ)
    }
    //dfJ.show(100)

    val checksumCount = dfJ.groupBy(col("columnName"),col("value")).agg(sum("pvod").as("checksum")).filter("checksum<>1").count()
    if (checksumCount != 0) throw new Exception("checksumJCount != 1")
     //end --create dfH

  }

  def initJTest(sc: SparkContext, sqlContext: SQLContext, countCluster: Int, dfN: DataFrame): DataFrame = {
    //create dfJ
    val schemaJ = StructType(
      StructField("cluster", IntegerType, false) ::
        StructField("columnName", StringType, false) ::
        StructField("value", StringType, false) ::
        StructField("pvod", DoubleType, false) :: Nil)
    var dfJ = sqlContext.createDataFrame(sc.emptyRDD[Row], schemaJ)

    val dfNDistinct = dfN.select(col("columnName"), col("value")).distinct()
    val columnNameValueDist = dfNDistinct
      .withColumn("cluster", lit(1: Int).cast(IntegerType))
      .withColumn("pvod", rand().cast(DoubleType))
      .select(col("cluster"),col("columnName"), col("value"),  col("pvod"))

    dfJ = dfJ.unionAll(columnNameValueDist)

    //dfJ.show(100)
    for (i <- 2 to countCluster) {
      val valueJ = dfJ
        .groupBy(col("columnName"), col("value")).agg(sum("pvod").as("sum_pvod"))
        .withColumn("cluster", lit(i: Int).cast(IntegerType))
        .withColumn("rand", rand().cast(DoubleType))
        .withColumn("pvod", myFunc(col("sum_pvod"), col("rand"), lit(i == countCluster: Boolean)))
        .select(col("cluster"),col("columnName"), col("value"),  col("pvod"))
      //valueJ.show(100)
      dfJ = dfJ.unionAll(valueJ)
    }
    //dfJ.show(100)

    val checksumCount = dfJ.groupBy(col("columnName"),col("value")).agg(sum("pvod").as("checksum")).filter("checksum<>1").count()
    if (checksumCount != 0) throw new Exception("checksumJCount != 1")

    //end --create dfH
    return dfJ
  }

  def H(sqlContext: SQLContext, dfQ:DataFrame,dfJ:DataFrame): DataFrame ={
/*
    val dfQJjoin = dfQ.join(dfJ, dfQ("cluster")===dfJ("cluster"))
    .withColumn("pvodQJ", dfQ("pvod").as("pvodQ")*dfJ("pvod").as("pvodJ"))
    dfQJjoin.show(100)
    println( new DateTime()+" - "+dfQJjoin.count())
    val dfH1 = dfQJjoin.groupBy(col("mac"),col("columnName"),col("value")).agg(sum("pvodQJ"))
    dfH1.show(100)
    println( new DateTime()+" - "+dfH1.count())
    */

    dfQ.registerTempTable("dfQ")
    dfJ.registerTempTable("dfJ")

    val query = """
                 select q.mac,q.cluster,j.columnName,j.value, j.pvod * q.pvod as pvodQJ from dfQ as q
                 left join dfJ as j on q.cluster = j.cluster
                """

    import sqlContext._
    val dfH = sql(query)
    println(dfH.count())
  dfH.show()

    return dfH
  }


  def myFunc = udf(
    { (c: Double, r: Double, isLastClaster: Boolean) =>
      val pvod = (1 - c)
      if (isLastClaster) pvod else pvod * r
    }
  )

}
