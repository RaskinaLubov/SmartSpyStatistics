/**
  * Created by Raslu on 10.02.2016.
  */
import java.sql.Timestamp
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}
import org.joda.time.{Period, Instant, Interval, DateTime}
import org.joda.time.format.{PeriodFormatterBuilder, DateTimeFormat}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SmartSpyStatisticsApp  extends App{
  val sparkConf = new SparkConf().setAppName("SmartSpy").setMaster("local[2]")
  val sc = new SparkContext(sparkConf)
  val rawPrimaryStat = sc.textFile("C:\\Users\\Raslu\\IdeaProjects\\SmartSpyStatistics\\src\\cont_cut.txt")

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


  val columnStat  = Array("msgType","streamType","spyVersion","playerUrl")
  val countCluster = 4

  val hms = new PeriodFormatterBuilder().minimumPrintedDigits(2).printZeroAlways().appendHours().appendSeparator(":").appendMinutes().appendSuffix(":").appendSeconds().toFormatter
  SmartSpyFunctions.writeCommonStatistics(primaryStatDF)
  var timeStart = new DateTime()

  val dfN = SmartSpyFunctions.initN(sc, sqlContext, primaryStatDF, columnStat)
  val periodN = new Period(timeStart, new DateTime()).normalizedStandard()
  println("periodN:" + hms.print(periodN)+" count" +dfN.count())

  timeStart = new DateTime()
  val dfQ = SmartSpyFunctions.initQ(sc, sqlContext, primaryStatDF, countCluster)
  val periodQ = new Period(timeStart, new DateTime()).normalizedStandard()
  println("periodQ:" + hms.print(periodQ)+" count" +dfQ.count())

  timeStart = new DateTime()
  val dfJ = SmartSpyFunctions.initJTest(sc, sqlContext, countCluster, dfN)
  val periodJ = new Period(timeStart, new DateTime()).normalizedStandard()
  println("periodJ:" + hms.print(periodJ)+" count" +dfJ.count())


  timeStart = new DateTime()
  val dfH = SmartSpyFunctions.H(sqlContext,dfQ,dfJ)
  val periodH = new Period(timeStart, new DateTime()).normalizedStandard()
  println("periodH:" + hms.print(periodH)+" count" +dfH.count())

}
