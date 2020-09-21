package com.paypal.QDVT
import java.nio.file.Paths
import java.util.Properties

import com.paypal.QDVT.DataDistribution.checkAndSelectTable
import com.paypal.QDVT.QuestUtils.{logger, rowCountFromFile}
import com.paypal.gimel.scaas.GimelQueryProcessor

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
      import sparkSession.implicits._
      val colselect = checkAndSelectTable(projectID: Int, subProjectID, pipelineID: Int, scenarioID: Int)
      //scenario_id,table_name,check_id,date_partition_columns
      val columnlist = colselect.map(x => (x.getInt(0), x.getString(1), x.getInt(2), x.getString(3))).collect.toList
      print(columnlist)

      //var columnCountList = new ListBuffer[String]()
      for (tbllist <- columnlist) {
        var rowCountList = new ListBuffer[String]()
        val tableNames = tbllist._2
        val check_id = tbllist._3
        val partitionColumn = tbllist._4
        val splitTable = tableNames.split("\\|\\|\\|")
        if (splitTable.length > 0) {
          for (tbl <- splitTable) {
            val checkTable = checkQueryOrTable(tbl, check_id, properties, partitionColumn)
            if (check_id == 12) {
              val rowCount = checkTable.collect().map(_.getLong(0)).mkString("")
              print("rowCount:", rowCount)
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
          println("The information are:" + tableNames + ":" + check_id.toString + ":", rowCount)
        }
      }
    }
    catch {
      case e: Exception => logger.error("[Error in crossComparison]" + e.getMessage)
    }
  }

  def checkAndSelectTable(projectID: Int, subProjectID: Int, pipelineID: Int, scenarioID: Int)(implicit sparkSession: SparkSession): DataFrame = {
    try {
      import sparkSession.sql
      val tableAavail = sql("select scenario_id,table_name,check_id,date_partition_columns from m360_quest.t_dq_trend_attribs where project_id=" + projectID + " and scenario_id=" + scenarioID + " and pipeline_id=" + pipelineID)
      val countTable = tableAavail.count()
      //logger.info("Table Name available " + countTable)
      if (countTable == 0) {
        break
      } else {
        return tableAavail
      }
    }
    catch {
      case e: Exception => logger.error("[Error in checkAndSelectTable]" + e.getMessage)
        throw e
    }
  }

  def checkQueryOrTable(tableName: String,check_id:Int,properties: Properties,partitionColumn:String)(implicit sparkSession: SparkSession): DataFrame = {
    if (tableName.contains("-{")){
      val query =tableName.split("-\\{")(1).dropRight(1)
      val tableEnv = tableName.split("-\\{}")
      val tblName="NA.NA"

       val check_name = getCheckName(check_id)
        val dfcount = tableEnv(0) match {
          case "horton" | "stampy" => sparkSession.sql(query)
          case "simba" => QuestUtils.execTDStmt(7, properties, tblName, query, Constants.QUERY_MD)
          case "jackal" => QuestUtils.execTDStmt(7, properties,tblName,query,Constants.QUERY_MD)
        }
       return dfcount
    }else
    {
      val table = tableName.split("-")
        //val combineTable= table(0)+"."+table(1)

      val recordCount = "select count(*) from " + table(1) + " where " + partitionColumn + "=(select max(" + partitionColumn + ") from " + table(1) + ")"
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
      return dfcount
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

 /** def getCount(check_name:String,tbName:String,query:String,properties: Properties,partitionColumn:String)(implicit sparkSession: SparkSession): DataFrame = {
    try {

      val glog = properties.get("custom.gimel.logging.level").toString
      val guser = properties.get("custom.gimel.jdbc.username").toString
      val genableQuery = properties.get("custom.gimel.jdbc.enableQueryPushdown").toString
      val gtype = properties.get("custom.gimel.jdbc.read.type").toString
      val gstrategy = properties.get("custom.gimel.jdbc.p.strategy").toString
      val gfile = properties.get("custom.gimel.jdbc.p.file").toString
      var gpart = List[String]()

      val recordCount = "select count(*) from " + tbName + " where " + partitionColumn + "=(select max(" + partitionColumn + ") from " + tbName + ")"
      val columncount ="select * from " + tbName + " limit 1"
      if (tbName == "NA") {
        val gsql1: String => DataFrame = GimelQueryProcessor.executeBatch(_: String, sparkSession)
        gsql1("set " + Constants.GIMEL_LOG_LEVEL + "=" + glog + ";set " + Constants.GIMEL_JDBC_USERNAME + "=" + guser + ";set " + Constants.GIMEL_PUSHDOWN + "=" + genableQuery + ";set " + Constants.GIMEL_READ_TYPE + "=" + gtype + ";set " + Constants.GIMEL_PART_COLS + "=" + gpart.mkString(","))
        gsql1(query)
      }
      else {
        //val dbName = tbName.split("\\.")(0)
        //val tblName = tbName.split("\\.")(1)
        val partQuery = check_environ match {
          case "Record Count"  => "select count(*) from " + tbName + " where " + partitionColumn + "=(select max(" + partitionColumn + ") from " + tbName + ")"
          case "Column Count" => "select * from " + tbName + " limit 1"
        }
        val gsql: String => DataFrame = GimelQueryProcessor.executeBatch(_: String, sparkSession)
        gsql("set " + Constants.GIMEL_LOG_LEVEL + "=" + glog + ";set " + Constants.GIMEL_JDBC_USERNAME + "=" + guser + ";set " + Constants.GIMEL_PUSHDOWN + "=" + genableQuery + ";set " + Constants.GIMEL_READ_TYPE + "=" + gtype)
        gsql(partQuery)
        //.collect().map(_.getString(0)).toList
      }
    }
    catch {
      case e: Exception => logger.error("[Error in getCount]" + e.getMessage)
        throw e
    }
  }**/
}
