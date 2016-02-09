/**
  * Created by Raslu on 10.02.2016.
  */
import java.sql.Timestamp

import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._

object SmartSpyStatisticsApp  extends App{
  val sparkConf = new SparkConf().setAppName("SmartSpy").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val rawPrimaryStat = sc.textFile("C:\\Users\\Raslu\\IdeaProjects\\SmartSpyStatistics\\src\\cont_cut_30000.txt")

  val dateFormat = DateTimeFormat.forPattern("DD/MM/YYYY HH:mm:ss.SSS")
  val timeFormat = DateTimeFormat.forPattern("mm:ss.SSS")

  val primaryStat = rawPrimaryStat.map(_.split(" ")).map(r => PrimaryStatistics(
    r(0).toInt,
    r(1),
    r(2),
    new Timestamp(DateTime.parse(r(3) + " " + r(4), dateFormat).toDate.getTime),
    new Timestamp(DateTime.parse(r(5), timeFormat).toDate.getTime),
    r(6),
    r(7),
    r(8).toInt,
    r(9).toInt,
    r(11),
    r(12).toInt,
    r(13).toInt,
    r(14).toInt,
    r(15).toInt,
    r(16).toInt,
    r(17).toDouble,
    r(18),
    r(19).toInt,
    r(20),
    r(21),
    new Timestamp(DateTime.parse(r(22) + " " + r(23), dateFormat).toDate.getTime),
    r(24),
    r(25),
    if(r(26).equals("X")){-1}else{r(26).toInt},
    if(r(27).equals("X")){-1}else{r(27).toInt},
    if(r(28).equals("X")){-1}else{r(28).toInt},
    if(r(29).equals("X")){-1}else{r(29).toInt},
    if(r(30).equals("X")){-1}else{r(30).toInt},
    r(32).toInt,
    r(33).toInt,
    r(34).toInt,
    r(35).toInt,
    r(36).toInt,
    r(37).toInt,
    r(38).toInt,
    r(39).toInt,
    r(40).toInt,
    r(41).toInt,
    r(42).toInt,
    r(43).toInt,
    r(44).toInt,
    r(45).toInt,
    r(46).toInt,
    r(47).toInt,
    r(48).toInt,
    r(49).toInt,
    r(50).toInt
  )
  )
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._
  val primaryStatDF = primaryStat.toDF()



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
    dfFilterPlayerUrl.groupBy("playerUrl").count().join(dfFilterPlayerUrl.agg(count("playerUrl").as("countAll")))

    writer.close()

    def logger = LoggerFactory.getLogger(this.getClass)
    logger.info("select data about 5 users")


  val macListDF = primaryStatDF.groupBy(col("mac").as("mac1")).count().orderBy(desc("count")).limit(10).select("mac1")
  val macDF = primaryStatDF.join(macListDF,macListDF("mac1") === primaryStatDF("mac")).select(primaryStatDF.col("*"))//.select(primaryStatDF.columns.mkString(", "))
  macDF.show
  //macDF.write.parquet("parquetTest")

}
