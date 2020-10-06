package com.paypal.QDVT
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import com.paypal.QDVT.QuestUtils.{logger, rowCountFromFile}
import com.paypal.QDVT.CrossEnvComparison.{checkAndSelectTable, checkQueryOrTable, _}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SPARK_REPO_URL, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.log4j.Logger
import com.paypal.utils._
object MultipleAttributes {
  val logger: Logger = Logger.getLogger(MultipleAttributes.getClass)
  def multipleAttributes(properties: Properties, projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int, scenarioName: String, batchDate: String, btchSeqID: Int, configDBName: String)(implicit sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val scenarioStartTime = DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT).format(LocalDateTime.now)
    try{

    val colselect = checkAndSelectTable(projectID: Int, subProjectID:Int, pipelineID: Int, scenarioID: Int)
    val columnlist = colselect.map(x => (x.getInt(0), x.getString(1), x.getInt(2), x.getString(3),x.getString(4))).collect.toList
    print(columnlist)
      var statusOutput = new ListBuffer[String]()
    for (tbllist <- columnlist) {
      var rowCountList = new ListBuffer[String]()
      var rowCountList2 = new ListBuffer[String]()
      var stringOutput = new ListBuffer[String]()
      val tableNames = tbllist._2
      val check_id = tbllist._3
      val partitionColumn = tbllist._4
      val maxVal = tbllist._5.toInt
      val splitTable = tableNames.split("\\|\\|\\|")
      //var dfCount =0
      if (splitTable.length > 0) {
        for (tbl <- splitTable) {
          val dfgrpCount = checkQueryOrTable(tbl, check_id, properties, partitionColumn, batchDate,projectID,subProjectID,pipelineID,scenarioID,btchSeqID,configDBName)
          dfgrpCount.cache()
          val stringJson = dfgrpCount.limit(maxVal).toJSON.collect.mkString("", ";", "") //value in history stats
          //QuestUtils.updateDQTrendStatsHist(projectID, subProjectID, pipelineID, batchDate, btchSeqID, configDBName)
          stringOutput += stringJson

          val jsonArray = dfgrpCount.limit(maxVal).collect.map(r => Map(dfgrpCount.columns.zip(r.toSeq): _*))
          val jsonSplit = stringJson.split(";")
          // var rowCountList2 = new ListBuffer[String]()
          rowCountList2.clear()
          if (jsonSplit.length > 0) {
            for (i <- jsonArray) {
              for (j <- i.values) {
                rowCountList += j.toString
                rowCountList2 += j.toString
              }
            }
          }
        }
        val rowcount = rowCountList.toList.distinct
        val rowJsonCount = rowCountList2.toList.distinct
        var status = ""
        //validation status table
        if (rowcount.length == rowJsonCount.length) {
          status = "Pass"
        } else {
          status = "Fail"
        }
        statusOutput += status
        val valStatDF = Seq((scenarioID,check_id,tableNames,"NA","NA","NA",stringOutput.toList.mkString("|||"),"NA","NA","NA",status,"NA","NA","NA","NA","NA","NA",batchDate,btchSeqID,scenarioName,check_id.toString)).toDF()
        QuestUtils.updateDQValidationStatus(configDBName,projectID,subProjectID,pipelineID,valStatDF)
        // QuestUtils.updateSQUIDValidationStatus(configDBName: String, projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int, scenarioName: String, batchDate: String, btchSeqID: Int, status: String, Constants.EMPTY_STRING, Constants.EMPTY_STRING, scenarioStartTime: String, scenarioEndTime: String)
      }
    }
      val statusCondition = statusOutput.toList.distinct
      val scenarioEndTime = DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT).format(LocalDateTime.now)
      if (statusCondition.contains("Fail")){
        val squidStatus ="Fail"
        QuestUtils.updateSQUIDValidationStatus(configDBName: String, projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int, scenarioName: String, batchDate: String, btchSeqID: Int, squidStatus: String, Constants.EMPTY_STRING, Constants.EMPTY_STRING, scenarioStartTime: String, scenarioEndTime: String)
      }else{
        val squidStatus ="Pass"
        QuestUtils.updateSQUIDValidationStatus(configDBName: String, projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int, scenarioName: String, batchDate: String, btchSeqID: Int, squidStatus: String, Constants.EMPTY_STRING, Constants.EMPTY_STRING, scenarioStartTime: String, scenarioEndTime: String)
      }
    }
      catch {
        case e: Exception =>
          logger.error("[Error in MulipleAttribute]" + e.getMessage)
          e.printStackTrace()
          val status = "Abort"
          val scenarioEndTime = DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT).format(LocalDateTime.now)
          QuestUtils.updateSQUIDValidationStatus(configDBName, projectID, subProjectID, pipelineID, scenarioID, scenarioName, batchDate, btchSeqID, status, Constants.EMPTY_STRING, Constants.EMPTY_STRING, scenarioStartTime, scenarioEndTime)
          throw e
      }
    }

}
