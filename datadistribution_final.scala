package com.paypal.QDVT
import java.util.Properties
import org.apache.spark.sql.functions.{col, _}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.language.{implicitConversions, postfixOps}
import java.util.{Calendar, GregorianCalendar}
import java.text.SimpleDateFormat
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.paypal.utils.Constants
import org.apache.spark.sql.types.DataTypes
import scala.util.control.Breaks.{break, _}
/** Sample Input for trend rule column -15%<->15% and for additional information column: lowrange,high range,seed value
 * will represent like 0<->10000<->500 */
object DataDistribution {
  val logger: Logger = Logger.getLogger(DataDistribution.getClass)
  def dataDistribution(properties: Properties, projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int, scenarioName: String, batchDate: String, btchSeqID: Int, configDBName: String)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val scenarioStartTime = DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT).format(LocalDateTime.now)
    try{
    val colselect = checkAndSelectTable(projectID:Int,scenarioID:Int,pipelineID:Int)
    val columnlist = colselect.map(x => (x.getString(0), x.getString(1), x.getString(2), x.getString(3), x.getInt(4))).collect.toList
    print(columnlist)
    val partition_column =columnlist(0)._3
    val table_name = columnlist(0)._4
    print("partition_column table_name ",partition_column,table_name)
    val hivedt = checkPartition(partition_column,table_name,batchDate)
    val digitmatch ="""(^[0-9]*)"""
    for ( columndata <- columnlist) {
      {
        print("arrayLengthBefore", columndata._2)
        val arrayLength = columndata._2.toString.split("<->")
        print("arrayLengthAfter", arrayLength)
        val columndataDFComp = getdateDataFrame(columndata._1, table_name, partition_column, batchDate)
        columndataDFComp.cache()
        //columndataDFComp.show(10, false)
        val table_count = columndataDFComp.count().toLong
        if (arrayLength.length == 3) {
          val lowRange = columndata._2.split("<->")(0)
          val highRange = columndata._2.split("<->")(1)
          val seedValue = columndata._2.split("<->")(2)
          if (lowRange.matches(digitmatch) && highRange.matches(digitmatch)) {
            val columndataDF = columndataDFComp.filter((col(columndata._1) >= lowRange) && (col(columndata._1) <= highRange))

            validatetheDf(configDBName, columndataDF, columndata, digitmatch, seedValue, projectID, subProjectID, pipelineID, btchSeqID, hivedt, table_name, table_count, scenarioID, batchDate)
          }
        } else {
          val seedValue = "NA"
          validatetheDf(configDBName, columndataDFComp, columndata, digitmatch, seedValue, projectID, subProjectID, pipelineID, btchSeqID, hivedt, table_name, table_count, scenarioID, batchDate)
        }
        val alertTable = getAlertData(projectID, subProjectID, pipelineID, scenarioID, table_name, columndata._1)
        val rules = alertTable.map(x => (x.getString(0), x.getString(1), x.getString(2))).collect.toList
        val rule = rules(0)._2.toString

        if (columndata._5.toInt > 0) {
          val dateId = "-" + columndata._5.toString
          val hivedtYesterday = dateConverstion(hivedt, dateId)
          val pastDF = getPastData(projectID, subProjectID, pipelineID, scenarioID, table_name, columndata._1, hivedtYesterday, hivedt)
          val currentDF = getCurrentData(projectID, subProjectID, pipelineID: Int, scenarioID, table_name, columndata._1, hivedt, btchSeqID)
          val trendRule = rules(0)._3 //.split(",")
          val lowTrendRule = trendRule.split("<->")(0).split("%")(0).toInt
          val highTrendrule = trendRule.split("<->")(1).split("%")(0).toInt

          val finalTrendStats = currentDF.join(pastDF, col("column_name") === col("past_column_name") &&
            col("column_value") === col("past_column_value"), "outer")

          val fillnaDf = finalTrendStats.na.fill("NA", Array("column_value")).na.fill("NA", Array("past_column_value"))
            .na.fill(0, Array("table_count")).na.fill(0, Array("past_table_count")).na.fill(0, Array("error_records_count"))
            .na.fill(0, Array("past_error_count")).na.fill(columndata._1, Array("column_name")).na.fill(columndata._1, Array("past_column_name"))

          val fillna = fillnaDf.withColumn("error_percentage", ((col("error_records_count") - col("table_count")) * 100 / col("table_count")))
            .withColumn("average_error_percentage", ((col("past_table_count") - col("past_error_count")) / col("past_error_count")) * 100)
          val trendDF = fillna.withColumn("trend_status", when(col("average_error_percentage").between(lowTrendRule, highTrendrule), "Pass").otherwise("Fail"))
            .withColumnRenamed("past_table_count", "average_table_count").withColumnRenamed("past_error_count", "average_error_records_count")
          //trendDF.select("scenario_id","check_id","table_name","filter_condition","column_name","column_value","table_count","error_records_count","error_percentage")
          breakable {
            for (i <- List("<=", ">=", "<", ">", "==")) {
              if (rule.contains(i)) {
                val percentValue = rule.split("%")(0).split(i)(1)
                val validationDF = matchCondition(i, trendDF, percentValue.toInt)
                val trendDFPart = validationDF.withColumn("threshold_percentage", lit(rule)).withColumn("comments", lit("NA"))
                  .withColumn("average_error_percentage",validationDF.col("average_error_percentage").cast(DataTypes.LongType))
                  .withColumn("batch_date", lit(hivedt)).withColumn("batch_sequence", lit(btchSeqID)).withColumn("trend_threshold_percentage", lit(trendRule))
                  .withColumn("scenario_name", lit(scenarioName)).withColumn("check_name", lit("NA"))
                  .withColumn("project_id", lit(projectID)).withColumn("sub_project_id", lit(subProjectID))
                  .withColumn("pipeline_id", lit(pipelineID)).withColumn("scenario_id", lit(scenarioID))
                  .withColumn("check_id", lit("NA")).withColumn("table_name", lit(table_name))
                  .withColumn("filter_condition", lit("NA"))
                  .select("scenario_id", "check_id", "table_name", "filter_condition", "column_name", "column_value", "table_count", "error_records_count", "error_percentage",
                    "threshold_percentage", "status", "average_table_count", "average_error_records_count", "average_error_percentage", "trend_threshold_percentage", "trend_status", "comments",
                    "batch_date", "batch_sequence", "scenario_name", "check_name", "project_id", "sub_project_id", "pipeline_id")
                //trendDFPart.show(14, false)
                QuestUtils.updateDQValidationStatus(configDBName, projectID, subProjectID, pipelineID, trendDFPart)
                break
              }
            }
          }
        } else {
          val currentDF = getCurrentData(projectID, subProjectID, pipelineID: Int, scenarioID, table_name, columndata._1, hivedt, btchSeqID)
          currentDF.show(6, false)
          val trendDF = currentDF.withColumn("error_percentage", ((col("table_count") - col("error_records_count")) / col("error_records_count")) * 100)
          trendDF.show(7, false)
          breakable {
            for (i <- List("<=", ">=", "<", ">", "==")) {
              if (rule.contains(i)) {
                val percentValue = rule.split("%")(0).split(i)(1)
                val validationDF = matchCondition(i, trendDF, percentValue.toInt)
                val nonpartDF = validationDF.withColumn("threshold_percentage", lit(rule)).withColumn("comments", lit("NA"))
                  .withColumn("batch_date", lit(hivedt)).withColumn("batch_sequence", lit(btchSeqID))
                  .withColumn("scenario_name", lit(scenarioName)).withColumn("check_name", lit("NA"))
                  .withColumn("project_id", lit(projectID)).withColumn("sub_project_id", lit(subProjectID))
                  .withColumn("pipeline_id", lit(pipelineID)).withColumn("scenario_id", lit(scenarioID))
                  .withColumn("check_id", lit("NA")).withColumn("table_name", lit(table_name))
                  .withColumn("filter_condition", lit("NA")).withColumn("trend_status", lit("NA"))
                  .withColumn("average_table_count", lit("NA")).withColumn("average_error_records_count", lit("NA"))
                  .withColumn("average_error_percentage", lit("NA")).withColumn("trend_threshold_percentage", lit("NA"))
                  .select("scenario_id", "check_id", "table_name", "filter_condition", "column_name", "column_value", "table_count", "error_records_count", "error_percentage",
                    "threshold_percentage", "status", "average_table_count", "average_error_records_count", "average_error_percentage", "trend_threshold_percentage", "trend_status", "comments",
                    "batch_date", "batch_sequence", "scenario_name", "check_name", "project_id", "sub_project_id", "pipeline_id")
                nonpartDF.show(13, false)
                QuestUtils.updateDQValidationStatus(configDBName, projectID, subProjectID, pipelineID, nonpartDF)
                break
              }
            }
          }
        }
      }
    }
   val statusDF = getStatus(configDBName, projectID, subProjectID, pipelineID,scenarioID,btchSeqID,hivedt)
      var status = "unknown"
      if (statusDF > 0){
         status = "Fail"
      }else{
         status = "Pass"
      }
      val scenarioEndTime = DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT).format(LocalDateTime.now)
      QuestUtils.updateSQUIDValidationStatus(configDBName, projectID, subProjectID, pipelineID, scenarioID, scenarioName, batchDate,btchSeqID, status, Constants.EMPTY_STRING,Constants.EMPTY_STRING, scenarioStartTime, scenarioEndTime)
      print(columnlist)
    }
    catch {
      case e: Exception =>
        logger.error("[Error in Data Distribution]" + e.getMessage)
        e.printStackTrace()
        val status = "Abort"
        val scenarioEndTime = DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT).format(LocalDateTime.now)
        QuestUtils.updateSQUIDValidationStatus(configDBName, projectID, subProjectID, pipelineID, scenarioID, scenarioName, batchDate, btchSeqID, status, Constants.EMPTY_STRING, Constants.EMPTY_STRING, scenarioStartTime, scenarioEndTime)
        throw e
    }
  }

  def validatetheDf(configDBName:String,columndataDF:DataFrame,columndata:(String,String,String,String,Int),digitmatch:String,seedValue:String,projectID:Int,subProjectID:Int,pipelineID:Int,btchSeqID:Int,hivedt:String,table_name:String,table_count:Long,scenarioID:Int,batchDate:String)(implicit sparkSession: SparkSession): Unit ={
    import sparkSession.implicits._

    val limitColumn = columndataDF.groupBy(columndata._1)
    .agg(count(columndata._1).alias("error_records_count"),max(columndata._1).alias("max"),min(columndata._1).alias("min"))

    if (limitColumn.take(5).length == 5){
    val nonrangecolumnvalue= columndataDF.withColumn("column_name",lit(columndata._1))//.drop(columndata._1)
    val nonrangecolumn = nonrangecolumnvalue.groupBy("column_name")
    .agg(count("column_name").alias("error_records_count"), min(columndata._1).alias("min"), max(columndata._1).alias("max"))
    val nonrangecolumnadded= nonrangecolumn.withColumn("column_value",lit("NA")).withColumn("batch_date",lit(hivedt))
      .withColumn("table_count",lit(table_count)).withColumn("project_id", lit(projectID))
      .withColumn("sub_project_id", lit(subProjectID)).withColumn("pipeline_id", lit(pipelineID)).withColumn("check_id", lit("NA"))
      .withColumn("batch_sequence",lit(btchSeqID)).withColumn("table_name",lit(table_name))
      .withColumn("filter_condition",lit("NA")).withColumn("scenario_id",lit(scenarioID))
      .select(Constants.SCENARIO_ID, Constants.CHECK_ID, Constants.TABLE_NAME, Constants.FILTER_CONDITION, Constants.COLUMN_NAME, Constants.COLUMN_VALUE, Constants.TABLE_COUNT, Constants.ERROR_REC_CNT, Constants.PROJECT_ID, Constants.SUB_PROJECT_ID, Constants.PIPELINE_ID, Constants.BATCH_DATE, Constants.BATCH_SEQUENCE)
      QuestUtils.updateDQTrendStatsHist(projectID, subProjectID, pipelineID, hivedt, btchSeqID, configDBName, nonrangecolumnadded)

  }
    else{
    val limitColumnRenamed= limitColumn.withColumnRenamed(columndata._1,"column_value").withColumn("column_name",lit(columndata._1))
      .withColumn("table_count",lit(table_count)).withColumn("project_id", lit(projectID))
      .withColumn("batch_date",lit(hivedt)).withColumn("sub_project_id", lit(subProjectID))
      .withColumn("pipeline_id", lit(pipelineID)).withColumn("check_id", lit("NA"))
      .withColumn("batch_sequence",lit(btchSeqID)).withColumn("table_name",lit(table_name))
      .withColumn("filter_condition",lit("NA")).withColumn("scenario_id",lit(scenarioID))
      .select(Constants.SCENARIO_ID, Constants.CHECK_ID, Constants.TABLE_NAME, Constants.FILTER_CONDITION, Constants.COLUMN_NAME, Constants.COLUMN_VALUE, Constants.TABLE_COUNT, Constants.ERROR_REC_CNT, Constants.PROJECT_ID, Constants.SUB_PROJECT_ID, Constants.PIPELINE_ID, Constants.BATCH_DATE, Constants.BATCH_SEQUENCE)
      QuestUtils.updateDQTrendStatsHist(projectID, subProjectID, pipelineID, hivedt, btchSeqID, configDBName, limitColumnRenamed)
  }

    if (seedValue.matches(digitmatch)){//columndata._2.toInt
    val rangeColumnTestCurrent = checkRangeSelectColumn(columndataDF,columndata._1.toString, seedValue.toInt,columndata._3.toString,hivedt)
     val histTrendDF = rangeColumnTestCurrent.withColumn("table_count",lit(table_count)).withColumn("project_id", lit(projectID))
        .withColumn("sub_project_id", lit(subProjectID))
        .withColumn("pipeline_id", lit(pipelineID)).withColumn("check_id", lit("NA"))
        .withColumn("batch_sequence",lit(btchSeqID)).withColumn("table_name",lit(table_name))
        .withColumn("filter_condition",lit("NA")).withColumn("scenario_id",lit(scenarioID))
       .select(Constants.SCENARIO_ID, Constants.CHECK_ID, Constants.TABLE_NAME, Constants.FILTER_CONDITION, Constants.COLUMN_NAME, Constants.COLUMN_VALUE, Constants.TABLE_COUNT, Constants.ERROR_REC_CNT, Constants.PROJECT_ID, Constants.SUB_PROJECT_ID, Constants.PIPELINE_ID, Constants.BATCH_DATE, Constants.BATCH_SEQUENCE)
      QuestUtils.updateDQTrendStatsHist(projectID, subProjectID, pipelineID, batchDate, btchSeqID, configDBName, histTrendDF)
  }
  }

  def checkAndSelectTable(projectID:Int,scenarioID:Int,pipelineID:Int)(implicit sparkSession: SparkSession): DataFrame={
    try {
      import sparkSession.sql
      val tableAavail = sql("select column_name,additional_info,date_partition_columns,table_name,number_of_partitions from m360_quest.t_dq_trend_attribs where project_id="+projectID+" and scenario_id="+scenarioID+" and pipeline_id="+pipelineID)
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

  def checkPartition(partition_column:String,table_name:String,batchdate:String)(implicit sparkSession: SparkSession):String  = {

    try {
      import sparkSession.sql
      val partitionValue = sql("select * from " + table_name+ " where "+partition_column+"='"+batchdate+"' limit 1") //.collect().map(_.getString(0)).mkString("")
      if (partitionValue.take(1).length == 1) {
        return partitionValue.select(col(partition_column)).collect().map(_.getString(0)).mkString("")
      }else{
        logger.error("[Table for the batch date is not Available ]")
        break
      }
    }
    catch {
      case e: Exception => logger.error("[Error in checkPartition]"+ e.getMessage)
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

  def checkRangeSelectColumn(columndataDF:DataFrame,column_name:String, interval:Int, dt:String,hivedt:String)(implicit sparkSession: SparkSession): DataFrame= {
    try{
      import sparkSession.implicits._
      val df = columndataDF.withColumn("range", col(column_name) - (col(column_name) % interval))
        .withColumn("column_value", concat($"range", lit(" <-> "), $"range" + interval))
      val rangeDF = df.groupBy($"column_value")
        .agg(count($"column_value").as("error_records_count"),min(col(column_name)).alias("min"),max(col(column_name)).alias("max"))
        .withColumn("column_name", lit(column_name))
        .withColumn("batch_date",lit(hivedt))//rangeDF.show()
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


  def getPastData(projectID:Int,subprojectID:Int,pipelineID:Int,scenarioID:Int,table_name:String,column_name:String,hivedtYesterday:String,hivedt:String)(implicit  sparkSession: SparkSession):DataFrame ={
    try {
      import sparkSession.sql
      import sparkSession.implicits._
      val column_count = sql("select * from m360_quest."+Constants.TBL_TREND_HIST_STATS + " where project_id="+projectID+
        " AND scenario_id="+scenarioID+" AND table_name='"+table_name+"' AND sub_project_id="+subprojectID+" AND column_name='"+column_name+"' AND pipeline_id="+ pipelineID+" AND batch_date >= '"+ hivedtYesterday + "' AND batch_date <= '"+hivedt+"'")
      val count_table = column_count.groupBy(Constants.COLUMN_NAME,Constants.COLUMN_VALUE)
        .agg(avg(Constants.TABLE_COUNT).alias("past_table_count"),avg(Constants.ERROR_REC_CNT).alias("past_error_count"))
      val count_rename = count_table.withColumnRenamed("column_name","past_column_name")
                        .withColumnRenamed("column_value","past_column_value")
      val chgdatatype=count_rename.withColumn("past_error_count",count_rename.col("past_error_count").cast(DataTypes.LongType))
        .withColumn("past_table_count",count_rename.col("past_table_count").cast(DataTypes.LongType))
        //.withColumn("past_error_count",count_rename.col("past_error_count").cast(DataTypes.LongType))
      return chgdatatype
    }
    catch {
      case e: Exception => logger.error("[Error in getPastData]"+ e.getMessage)
        throw e
    }
  }

 def getAlertData(projectID:Int,subprojectID:Int,pipelineID:Int,scenarioID:Int,table_name:String,column_name:String)(implicit sparkSession: SparkSession):DataFrame ={
   try{
   import sparkSession.sql
    val getAlertInfo = sql("select column_value,rule,trend_rule from m360_quest.t_alerts where column_name='"+column_name+"' AND project_id="+projectID+ " AND sub_project_id="+subprojectID+" AND pipeline_id="+ pipelineID  )
    return getAlertInfo
 }
   catch {
     case e: Exception => logger.error("[Error in getAlertData]"+ e.getMessage)
       throw e
   }
 }

  def getCurrentData(projectID:Int,subprojectID:Int,pipelineID:Int,scenarioID:Int,table_name:String,column_name:String,batchDate:String,btchSeqID:Int)(implicit sparkSession: SparkSession):DataFrame={
  try {
    import sparkSession.sql
    val column_count = sql("select * from m360_quest."+Constants.TBL_TREND_HIST_STATS + " where project_id="+projectID+ " AND column_name='"+column_name+"' AND batch_sequence="+btchSeqID+" AND  sub_project_id="+subprojectID+" AND pipeline_id="+ pipelineID+" AND batch_date= '"+batchDate+"'")
    return column_count
  }
  catch {
    case e: Exception => logger.error("[Error in getCurrentData]"+ e.getMessage)
      throw e
  }
}
  def getStatus(configDBName:String, projectID:Int, subProjectID:Int, pipelineID:Int,scenarioID:Int,btchSeqID:Int,hivedt:String)(implicit sparkSession: SparkSession):Long={

    try {
      import sparkSession.sql
      val column_count = sql("select column_name,status,trend_status from "+configDBName+"."+Constants.TBL_DQ_TREND_VS + " where (status='Fail' OR trend_status='Fail') AND project_id="+projectID+ " AND sub_project_id="+subProjectID+" AND pipeline_id="+ pipelineID+" AND scenario_id="+scenarioID+" AND batch_sequence="+ btchSeqID +" AND batch_date= '"+hivedt+"'")
      //column_count.show(20,false)
      val count_table = column_count.count()
      return count_table
    }
    catch {
      case e: Exception => logger.error("[Error in getStatus]"+ e.getMessage)
        throw e
    }
  }

  def matchCondition(i:String,trendDF:DataFrame,percentValue:Int): DataFrame ={
    val validationDF = i match {
      case "<=" => trendDF.withColumn("status",when(col("error_percentage") <= percentValue,"Pass").otherwise("Fail"))
      case ">=" => trendDF.withColumn("status",when(col("error_percentage") >= percentValue,"Pass").otherwise("Fail"))
      case "<" => trendDF.withColumn("status",when(col("error_percentage") < percentValue,"Pass").otherwise("Fail"))
      case ">" => trendDF.withColumn("status",when(col("error_percentage") > percentValue,"Pass").otherwise("Fail"))
      case "==" => trendDF.withColumn("status",when(col("error_percentage") === percentValue,"Pass").otherwise("Fail"))
      case _ => trendDF.withColumn("status",lit("NA"))
    }
    return validationDF
  }
}

