package QUEST
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

import scala.util.control.Breaks._
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode, SparkSession}
import java.util.{Calendar, GregorianCalendar}
import java.text.SimpleDateFormat
import org.apache.spark.sql.types.{DataTypes, IntegerType}
object Datadistribution {
  val logger: Logger = Logger.getLogger(testquest.getClass)

  def main(args: Array[String]): Unit = {
    val table_name =args(0)
    val check_id = args(1).toInt
    val date_id = args(2)
    val sparkConf = new SparkConf().setAppName("DataDistribution")//.setMaster(master = "local")
    implicit val sc: SparkContext = new SparkContext(sparkConf)
    implicit val hiveContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    //val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    implicit val spark = SparkSession.builder.appName("DataDistribution")//.master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", "true").config("hive.exec.dynamic.partition.mode", "nonstrict")
      .getOrCreate()

    implicit val sqlContext: SQLContext = spark.sqlContext
    import spark.implicits._
    import spark.sql
      val colselect = checkAndSelectTable(table_name:String,check_id:Int)
      //List((Age,5,year), (salary,1000,year), (gender,NA,year), (Id,NA,year))

      val columnlist = colselect.map(x => (x.getString(0), x.getString(1), x.getString(2))).collect.toList
      val partition_column =columnlist(0)._3
      val hivedt = checkPartition(partition_column, table_name)//
      var hivedtYesterday= dateConverstion(hivedt,date_id)
      var count_var = 0
      var check_past_table_count = check_past_table(hivedtYesterday,table_name,partition_column)

    //Check the yesterday table is available in any of the last 5 days
      while (check_past_table_count == 0){
        hivedtYesterday= dateConverstion(hivedtYesterday,date_id)
        check_past_table_count = check_past_table(hivedtYesterday,table_name,partition_column)
        count_var = count_var + 1
        if (count_var > 7){
          break
        }
      }
      logger.info("hive yesterday date"+hivedtYesterday)
      val digitmatch ="""(^[0-9]*)"""
      for ( columndata <- columnlist)
      {
        var checkdata = "NA"
        val columndataDF = getdateDataFrame(columndata._1,table_name,partition_column,hivedt)
        val hiveyesDF = getdateDataFrame(columndata._1,table_name,partition_column,hivedtYesterday)

        val limitColumn = columndataDF.groupBy(columndata._1)
            .agg(count(columndata._1).alias("count"),max(columndata._1).alias("max"),min(columndata._1).alias("min"))
        //limitColumn.length
        val limitColumnYesterday  = hiveyesDF.groupBy(columndata._1)
            .agg(count(columndata._1).alias("past_count"),max(columndata._1).alias("past_max"),min(columndata._1).alias("past_min"))//.limit(4)

      /** if the limit of distinct goes more than 4 first condition and else part will do wholesome count and that will triggered for range operation if
      the column is opted **/
      if (limitColumn.take(4).length == 4 && limitColumnYesterday.take(4).length==4){
        val nonrangecolumnvalue= columndataDF.withColumn("column_name",lit(columndata._1))//.drop(columndata._1)
        val nonrangecolumn = nonrangecolumnvalue.groupBy("column_name")
                            .agg(count("column_name").alias("count"), min(columndata._1).alias("min"), max(columndata._1).alias("max"))
        val nonrangecolumnadded= nonrangecolumn.withColumn("column_value",lit("NA")).withColumn("dt",lit(hivedt))

        val pastnonrangecolumnvalue = hiveyesDF.withColumn("past_column_name",lit(columndata._1))//.drop(columndata._1)
        val pastnonrangecolumn= pastnonrangecolumnvalue.groupBy("past_column_name")
                            .agg(count("past_column_name").alias("past_count"),min(columndata._1).alias("past_min"), max(columndata._1).alias("past_max"))
        val pastnonrangecolumnadded = pastnonrangecolumn.withColumn("past_column_value",lit("NA")).withColumn("past_dt",lit(hivedtYesterday))
        val past_current= nonrangecolumnadded.join(pastnonrangecolumnadded,col("column_name")===col("past_column_name")
          && col("column_value")===col("past_column_value"),"outer")
       // updateDataDistribution(rangewholeCount)
       // past_current.show(10,false)
        updateDataDistribution(past_current)
      }
      else{
          /** val totalDF = columndataDF.count()
           val oneTable = columndataDF.withColumn("column_name",lit(columndata._1)).drop(columndata._1)
           val oneTabDf= oneTable.withColumn("column_value",lit("NA")).withColumn("dt",lit(hivedt)).withColumn("count",lit(totalDF))
             .withColumn("past_column_value",lit("NA")).withColumn("past_dt",lit(hivedtYesterday))
             .withColumn("past_count",lit("NA")).withColumn("past_column_name",lit("NA"))
          updateDataDistribution(oneTabDf)**/
        val limitColumnRenamed= limitColumn.withColumnRenamed(columndata._1,"column_value").withColumn("column_name",lit(columndata._1))
          .withColumn("dt",lit(hivedt))
        val limitColumnRenamedYesterday = limitColumnYesterday.withColumnRenamed(columndata._1,"past_column_value").withColumn("past_column_name",lit(columndata._1))
          .withColumn("past_dt",lit(hivedtYesterday))
        val rangewholeCount = limitColumnRenamed.join(limitColumnRenamedYesterday,col("column_name")===col("past_column_name")
          && col("column_value")===col("past_column_value"),"outer")
        updateDataDistribution(rangewholeCount)

      }
      if (columndata._1.matches( "revenue_usd_amt_1m")){ //columndata._1.matches("salary")
        checkdata = "500000"
      }
      if (checkdata.matches(digitmatch)){//columndata._2.toInt
        val rangeColumnTestCurrent = checkRangeSelectColumn(columndataDF,columndata._1.toString, checkdata.toInt,columndata._3.toString,hivedt)
        val rangeColumnRenamePast = checkRangeSelectColumn(hiveyesDF,columndata._1.toString, checkdata.toInt,columndata._3.toString,hivedtYesterday)
        val rangeColumnTestPast = rangeColumnRenamePast.withColumnRenamed("count","past_count").withColumnRenamed("column_value","past_column_value")
          .withColumnRenamed("column_name","past_column_name").withColumnRenamed("dt","past_dt")
          .withColumnRenamed("max","past_max").withColumnRenamed("min","past_min")
        val rangewholeCount = rangeColumnTestCurrent.join(rangeColumnTestPast,col("column_name")===col("past_column_name")
          && col("column_value")===col("past_column_value"),"outer")
       // rangewholeCount.show(30,false)
        updateDataDistribution(rangewholeCount)

      }
    }
    print(columnlist)
  }

  def checkAndSelectTable(table_name:String,check_id:Int)(implicit sparkSession: SparkSession): DataFrame={
    try {
      import sparkSession.sql
      //val loop = new Breaks; //m360_quest.t_dq_trend_attribs metatable
      val tableAavail = sql("select column_name,additional_info,date_partition_columns from m360_quest.t_dq_trend_attribs where check_id="+check_id+" and table_name='" + table_name + "'")
      //val tableAavail = sql("select column_name,additional_info,date_partition_columns from metatable where  table_name='" + table_name + "'")
      val countTable =tableAavail.count()
      logger.info("Table Name available " + countTable)
      if (countTable==0) {
        break
    }else{
        return tableAavail
      }
    }

    catch {
      case e: Exception => logger.error("[Error in checkAndSelectTable]" + e.getMessage)
        throw e
    }
  }

  def updateDataDistribution(nonrangecolumnadded:DataFrame)(implicit sparkSession: SparkSession): Unit = {
    try {
      //sparkSession.implicits._
      val table_location = "/x/home/pp_risk_grs_batch_qa/ailango/mailtest"//
      //val table_location= sparkSession.sharedState.externalCatalog.getTable("default","datarangetest34599").location.toString+"/"
     // nonrangecolumnadded.show(20,false)
      //nonrangecolumnadded.write.partitionBy("dt").mode(SaveMode.Append).format("hive").saveAsTable("datarangetest3458702")
      print("table_location",table_location)
      nonrangecolumnadded.select("dt","column_name","column_value","count","min","max","past_dt","past_column_name","past_column_value","past_count","past_min","past_max").show(50,false)
      /** .write.format("csv")
        .option("header","true")
        .option("delimiter",",")
        .mode(saveMode = "append")
        .csv(table_location+"/target/files/logs/final")**/
      //nonrangecolumnadded.show(20,false) **/

      logger.info("Record inserted into validationstatus")
    } catch {
      case e: Exception => logger.error("[Error in updateValidationStatus]" + e.getMessage)
        throw e
    }
  }

  def checkRangeSelectColumn(columndataDF:DataFrame,column_name:String, interval:Int, dt:String,hivedt:String)(implicit sparkSession: SparkSession): DataFrame= {
    try{
    import sparkSession.implicits._
    val df = columndataDF.withColumn("range", col(column_name) - (col(column_name) % interval))
      .withColumn("column_value", concat($"range", lit(" <-> "), $"range" + interval))
    val rangeDF = df.groupBy($"column_value")
              .agg(count($"column_value").as("count"),min(col(column_name)).alias("min"),max(col(column_name)).alias("max"))
              .sort($"column_value").withColumn("column_name", lit(column_name))
    .withColumn("dt",lit(hivedt))//rangeDF.show()
    return rangeDF
    }
    catch {
      case e: Exception => logger.error("[Error in checkRangeSelectColumn]" + e.getMessage)
        throw e
    }
  }

  def getdateDataFrame(columnName:String,tableName:String,partitionColumn:String, tableDate:String )(implicit sparkSession: SparkSession):DataFrame={
    try {
      import sparkSession.sql
      val dateDF = sql("select " + columnName + " from " + tableName + " where " + partitionColumn + "='" + tableDate + "'")
      return dateDF
    }
    catch {
      case e: Exception => logger.error("[Error in getdateDataFrame]" + e.getMessage)
        throw e
    }
  }

  def checkPartition(partition_column:String,table_name:String)(implicit sparkSession: SparkSession):String  = {

    try {
      import sparkSession.sql
      val partitionValue = sql("select max(" + partition_column + ") from " + table_name).collect().map(_.getString(0)).mkString("")
      return partitionValue
    }
    catch {
      case e: Exception => logger.error("[Error in checkPartition]"+ e.getMessage)
        throw e
    }
  }

  def check_past_table(hivedtYesterday:String,table_name:String, partition_column:String)(implicit sparkSession: SparkSession):Long ={
      try {
        import sparkSession.sql
        val column_count = sql("select * from "+ table_name + " where "+ partition_column+" = '"+ hivedtYesterday + "' limit 1")
         val count_table = column_count.count()
        return count_table
      }
      catch {
        case e: Exception => logger.error("[Error in check_past_table]"+ e.getMessage)
          throw e
      }
    }


  def dateConverstion(cDay: String, date_arg: String): String = {
    try{
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val d = new SimpleDateFormat(("yyyy-MM-dd"))
    val gc = new GregorianCalendar()
    val myDate = dateFormat.parse(cDay)
    gc.setTime(myDate)
    d.format(myDate)
    gc.roll(Calendar.DAY_OF_YEAR, date_arg.toInt)
    gc.get(Calendar.DATE)
    val yesterday = gc.getTime()
    val previousDate = d.format(yesterday)
    println("mydate", previousDate)
    return previousDate.toString
    }
    catch {
      case e: Exception => logger.error("[Error in dateConverstion]"+e.getMessage)
        throw e
    }
  }

}

///m360_qa_current.fact_m360_product_usage_prod
