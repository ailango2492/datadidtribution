package com.paypal.QDVT
import java.nio.file.Paths
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SPARK_REPO_URL, SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.log4j.Logger
import com.paypal.utils._

object CrossEnvComparison {
  val logger: Logger = Logger.getLogger(CrossEnvComparison.getClass)

  def crossComparison(properties: Properties, projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int, scenarioName: String, batchDate: String, btchSeqID: Int, configDBName: String)(implicit sparkSession: SparkSession): Unit = {
    try {
      val scenarioStartTime = DateTimeFormatter.ofPattern(Constants.TIMESTAMP_FORMAT).format(LocalDateTime.now)
      import sparkSession.implicits._
      val colselect = checkAndSelectTable(projectID: Int, subProjectID, pipelineID: Int, scenarioID: Int)
      //scenario_id,table_name,check_id,date_partition_columns
      val columnlist = colselect.map(x => (x.getInt(0), x.getString(1), x.getInt(2), x.getString(3))).collect.toList
      print(columnlist)
      var statusOutput = new ListBuffer[String]()
      //var columnCountList = new ListBuffer[String]()
      for (tbllist <- columnlist) {
        var rowCountList = new ListBuffer[String]()
        val tableNames = tbllist._2
        val check_id = tbllist._3
        val partitionColumn = tbllist._4
        val splitTable = tableNames.split("\\|\\|\\|")
        if (splitTable.length > 0) {
          for (tbl <- splitTable) {
            val checkTable = checkQueryOrTable(tbl,check_id,properties,partitionColumn,batchDate,projectID,subProjectID,pipelineID,scenarioID,btchSeqID,configDBName)
            if (check_id == 12) {
              val rowCount = checkTable.collect().map(_.getLong(0)).mkString("")
              print("rowCount:", rowCount)
              //send the rowCount to trend history
              rowCountList += rowCount
            } else {
              println("row columns", checkTable.columns.length.toString)
              rowCountList += checkTable.columns.length.toString
            }
          }
          val rowCount = rowCountList.toList.distinct
          print(rowCount)
          var status = "NA"
          if (rowCount.length > 1) {
            status = "Fail"
          }
          else {
            status = "Pass"
          }
         // import sparkSession.implicits._
         statusOutput += status
         val valStatDF = Seq((scenarioID,check_id,tableNames,"NA","NA","NA",rowCountList.toList.mkString("||"),"NA","NA","NA",status,"NA","NA","NA","NA","NA","NA",batchDate,btchSeqID,scenarioName,check_id.toString)).toDF()
          println("The information are:" + tableNames + ":" + check_id.toString + ":", rowCount)
          //send the status to validation status table
          QuestUtils.updateDQValidationStatus(configDBName,projectID,subProjectID,pipelineID,valStatDF)
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
      case e: Exception => logger.error("[Error in crossComparison]" + e.getMessage)
    }
  }

  def checkAndSelectTable(projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int)(implicit sparkSession: SparkSession): DataFrame = {
    try {
      import sparkSession.sql
      val tableAvail = sql("select scenario_id,table_name,check_id,date_partition_columns,filter_condition from m360_quest.t_dq_trend_attribs where project_id=" + projectID + " and scenario_id=" + scenarioID + " and pipeline_id=" + pipelineID)
      val countTable = tableAvail.count()
      //logger.info("Table Name available " + countTable)
      if (countTable == 0) {
        break
      } else {
        tableAvail
      }
    }
    catch {
      case e: Exception => logger.error("[Error in checkAndSelectTable]" + e.getMessage)
        throw e
    }
  }

  def checkQueryOrTable(tableName: String,check_id:Int,properties:Properties,partitionColumn:String,batchDate:String,projectID:Int,subProjectID:Int,pipelineID:Int,scenarioID:Int,btchSeqID:Int,configDBName:String)(implicit sparkSession: SparkSession): DataFrame = {
    if (tableName.contains("-{")){
      var query =tableName.split("-\\{")(1).dropRight(1)
      val tableEnv = tableName.split("-\\{")
      val tblName="NA.NA"
       query = query.replace(Constants.BATCH_DATE, "'" + batchDate + "'")
       //val check_name = getCheckName(check_id)
        val dfcount = tableEnv(0) match {
          case "horton" | "stampy" => sparkSession.sql(query)
          case "simba" | "jackal" => QuestUtils.execTDStmt(subProjectID, properties, tblName, query, Constants.QUERY_MD)
        //  case "jackal" => QuestUtils.execTDStmt(7, properties,tblName,query,Constants.QUERY_MD)
        }
        dfcount
    }else
    {
      val table = tableName.split("-")
      //val combineTable= table(0)+"."+table(1)
      breakable{if (!QuestUtils.tblAvlCheck(properties,projectID,subProjectID,pipelineID,scenarioID,table(1),batchDate,btchSeqID,configDBName)){
        logger.error("Table Not available ")
        break
      }}

      val recordCount = partitionColumn match {
        case Constants.NA => "select count(*) from " + table(1)
        case _ =>   "select count(*) from " + table(1) + " where " + partitionColumn + "= '"+batchDate+"'"
      }
        //val recordCount = "select count(*) from " + table(1) + " where " + partitionColumn + "=(select max(" + partitionColumn + ") from " + table(1) + ")"
      val columncount ="select * from " + table(1) + " limit 1"
      val check_name = getCheckName(check_id)
      val query ="NA"
      val partQuery = check_name match {
        case "Record Count" => recordCount
        case "Column Count" => columncount
      }
        val dfcount = table(0) match {
        case "horton" | "stampy" => sparkSession.sql(partQuery)//.collect().map(_.getLong(0)).mkString(Constants.EMPTY_STRING)
        case "simba" => QuestUtils.execTDStmt(7, properties, table(1), partQuery.replace(table(1), Constants.UDC_TD_SIMBA + Constants.DOT + table(1)), Constants.QUERY_MD)
        case "jackal" => QuestUtils.execTDStmt(7, properties, table(1), partQuery.replace(table(1), Constants.UDC_TD_JACKAL + Constants.DOT + table(1)), Constants.QUERY_MD)
      }
     // val dfcount = getCount(check_name,tableName,query,properties,partitionColumn)
      dfcount
    }
  }

  def getCheckName(check_id: Int)(implicit sparkSession: SparkSession):String ={
    try{
      import sparkSession.sql

       val check_name=sql("select check_name from m360_quest.t_dq_check_lkp where check_id="+check_id).collect().map(_.getString(0)).mkString("")
        return check_name
    }
    catch {
      case e: Exception => logger.error("[Error in getCheckName]" + e.getMessage)
        throw e
    }
  }
}
