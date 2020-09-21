package com.paypal.jobs

import java.nio.file.Paths
import java.util.Properties
import scala.util.control.Breaks._
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.log4j.Logger
import scala.language.postfixOps
import com.paypal.QDVT._
import com.paypal.utils._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

object TestRunner {
  val logger: Logger = Logger.getLogger(TestRunner.getClass)

  def main(args: Array[String]): Unit = {

    if ((args.length < 5) || (args(0) == "" || args(0) == null) || (args(1) == "" || args(1) == null) || (args(2) == "" || args(2) == null)) {
      throw new IllegalArgumentException("Must provide projectId, subProjectId, pipelineId, batchdate and skiptool arguments as 'N' or blank. To skip SQUID provide skiptool  'Y'")
    }
    val projectId = args(0)
    val subProjectId = args(1)
    val pipelineId = args(2)
    val batchDate = args(3)
    val skiptool = args(4)

    val t1 = System.nanoTime
    logger.info("Start Time " + t1)
    val sparkConf = new SparkConf().setAppName("SQUID_" + projectId + "_" + subProjectId + "_" + pipelineId + "_" + batchDate)

    implicit val sc: SparkContext = new SparkContext(sparkConf)
    implicit val hiveContext: HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    implicit val sparkSession: SparkSession = SparkSession.builder().appName("SQUID")
      .enableHiveSupport()
      .getOrCreate()
    implicit val sqlContext: SQLContext = sparkSession.sqlContext
    import sparkSession.implicits._
    val hdfs = org.apache.hadoop.fs.FileSystem.newInstance(sparkSession.sparkContext.hadoopConfiguration)
    val config = new org.apache.hadoop.conf.Configuration()

   // val propFile = "/user/pp_risk_grs_batch_qa/squid.properties"
   val propFile = "/user/pp_risk_grs_batch_qa/ailango/squid.properties"
    val properties = Utilities.loadProperties(propFile)

    val configDBName = properties.get("custom.hive.db.name").toString
    logger.info("configDBName " + configDBName)
    val projectDF = QuestUtils.getProject(configDBName, projectId, subProjectId)
    val projectParams = projectDF.select(Constants.SOURCE, Constants.FREQ, Constants.IS_ACTIVE, Constants.TO_DL_LIST, Constants.CC_DL_LIST, Constants.BCC_DL_LIST, Constants.EMAIL_COMM_REQ_FLAG, Constants.MAIL_ATTACH_CATEGORY).collectAsList()
    val (source, frequency, projActiveStatus, to_dl_list, cc_dl_list, bcc_dl_list, email_comm_reqd_flag, mail_attach_category) = (projectParams.get(0)(0).toString, projectParams.get(0)(1).toString, projectParams.get(0)(2).toString, projectParams.get(0)(3).toString.split("\\|\\|\\|").mkString(","), projectParams.get(0)(4).toString.split("\\|\\|\\|").mkString(","), projectParams.get(0)(5).toString.split("\\|\\|\\|").mkString(","), projectParams.get(0)(6).toString, projectParams.get(0)(7).toString)

    if (projActiveStatus.equalsIgnoreCase(Constants.NO) || projActiveStatus.isEmpty) {
      logger.error("Project is not active. Exit the process ")
      break()
    }
    val configDF = QuestUtils.getConfigDF(configDBName, projectId, subProjectId, pipelineId)
    configDF.persist(StorageLevel.MEMORY_ONLY_SER)
    logger.info("config attributes")
    configDF.show()

    val projScenarioDF = QuestUtils.getProjectScenarioMap(configDBName, projectId, subProjectId, pipelineId, Constants.YES, batchDate, Constants.YES)
    projScenarioDF.persist(StorageLevel.MEMORY_ONLY_SER)
    val scenarioIdList = projScenarioDF.select("scenario_id").orderBy("sequence_id").map(r => r.getString(0)).collect().toList
    logger.info("scenarioIdList " + scenarioIdList)
    logger.info("Check scenario id exists in the scenario table")
    if (scenarioIdList.isEmpty) {
      logger.error("Scenarios not exists in table for the project.")
      break()
    }

    val batchSeqId = QuestUtils.generateBatchSeqId(configDBName, projectId, subProjectId, pipelineId, batchDate)
    //hskp
    val deleteoldhdfsfilespath = Paths.get("/user/pp_risk_grs_batch_qa/m360/mc/").toString
    val fs = new Path(deleteoldhdfsfilespath).getFileSystem(config)
    if (fs.exists(new Path(deleteoldhdfsfilespath))) {
      logger.info("delete the 3rd party files in hdfspath if exists")
      fs.delete(new Path(deleteoldhdfsfilespath), true)
    }

    try {
      if (skiptool == "Y") {
        logger.info("Skipping SQUID execution")
        if (args(5).equalsIgnoreCase("request")) {
          StarTestRunner.main(Array(projectId, subProjectId, pipelineId, batchDate, "request"))
        } else if (args(5).equalsIgnoreCase("response")) {
          StarTestRunner.main(Array(projectId, subProjectId, pipelineId, batchDate, "response"))
        } else {
          logger.error("Expecting argument for STAR")
        }
      }
      else {

        val thirdPartyFilePath = configDF.select("3rdparty_file_path").collect().map(_.getString(0)).mkString("")
        val zipSpecList = configDF.select("compressedspecificlist").collect().map(_.getString(0)).mkString("")
        val fileSpecList = configDF.select("filespecificlist").collect().map(_.getString(0)).mkString("")
        val fileExtension = configDF.select("fileextension").collect().map(_.getString(0)).mkString("")
        val delimiter = configDF.select("delimiter").collect().map(_.getString(0)).mkString("")
        val compressed = configDF.select("compressed").collect().map(_.getString(0)).mkString("")
        val hdfsPathValue = configDF.select("hdfs_path").collect().map(_.getString(0)).mkString("")
        val sourceType = configDF.select("sourcetype").collect().map(_.getString(0)).mkString("").toLowerCase

        var dataDF = sparkSession.emptyDataFrame
        var fileNameCheckOp = List((List(""), ""))
        var fileCount = 0l

        breakable {
          for (scenarioId <- scenarioIdList) {
            logger.info("scenarioId " + scenarioId)
            //scenarioName - temp solution
            val scenarioName = QuestUtils.getScenarioName(configDBName, scenarioId)
            val functionName = QuestUtils.getFunctionName(configDBName, projectId, subProjectId, scenarioId)
            logger.info(" functionName " + functionName)
            functionName match {
              case "fileNameCheck" => fileNameCheckOp = FileNameCheck.fileNameCheck(configDBName, projectId, subProjectId, pipelineId, scenarioId, scenarioName, batchDate, batchSeqId, thirdPartyFilePath, zipSpecList, fileSpecList, fileExtension, delimiter, compressed, hdfsPathValue, "Y")

              case "fileMoveCheck" => FileMove.fileMoveCheck(configDBName, projectId, subProjectId, pipelineId, scenarioId, scenarioName, batchDate, batchSeqId, thirdPartyFilePath, compressed, hdfsPathValue, "Y", zipSpecList, fileSpecList, fileExtension, fileNameCheckOp)

              case "schemaInference" => val result = SchemaInference.schemaInference(config, configDBName, projectId, subProjectId, pipelineId, scenarioId, scenarioName, batchDate, batchSeqId, configDF, projScenarioDF, fileExtension)
                if (!result._1) {
                  break()
                }
                dataDF = fileExtension match {
                  case "json" => var df = result._2.withColumn(Constants.ROW_INDEX, row_number().over(Window.orderBy(monotonically_increasing_id())))
                    val dfCols = df.schema.fields.map(name => name.name)
                    val arrayTypeCols = df.schema.fields.filter(_.dataType.toString containsSlice ("ArrayType")).map(name => name.name).toList
                    df = df.select(dfCols.map(col): _*)
                    fileCount = df.count
                    for (i <- arrayTypeCols) {
                      df = df.withColumn(i, explode(col(i)))
                    }
                    var explodeList = List[String]()
                    for (i <- df.schema) {
                      if (i.dataType.toString containsSlice ("StructType")) {
                        explodeList = (i.dataType.catalogString.toString.replace("struct<", i.name + ".").replace(">", "").replace(",", "," + i.name + ".")).mkString("").split(",").toList
                      }
                    }
                    for (i <- explodeList) {
                      df = df.withColumn(i.split(":")(0).replace(".", "~"), col(i.split(":")(0)))
                    }
                    df.drop(arrayTypeCols: _*)
                  case _ => result._2
                }

              case "mergeDataOnly" => dataDF = SchemaInference.mergeDataOnly(config, configDBName, projectId, subProjectId, pipelineId, scenarioId, scenarioName, batchDate, batchSeqId, configDF, projScenarioDF, fileExtension)

              case "dataConsistencyCheck" => DataConsistency.dataConsistencyCheck(config, configDBName, projectId, subProjectId, pipelineId, scenarioId, scenarioName, batchDate, batchSeqId, delimiter)

              case "DQChecks" => DataQualityCheck.DQChecks(properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, dataDF, batchDate, batchSeqId.toInt, configDBName, scenarioName, fileExtension, fileCount)

              case "calculateThresold" => QuestUtils.calculateThresold(config, configDBName, projectId, subProjectId, pipelineId, scenarioId, scenarioName, batchDate, batchSeqId, configDF, dataDF)

              case "tableTrend" => TableTrending.tableTrend(properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)

              case "columnTrend" => ColumnTrending.columnTrend(properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)

              case "objectExistence" => ObjectAvailability.objAvlCheck(properties: Properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)

              case "columnValueTrend" => ColumnValueTrending.tblColValTrendNPartns(properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)

              case "identifyZeroPartns" => IdentifyZeroPartitions.identifyZeroPartns(properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)

              case "customQuery" => CustomQuery.queryResult(properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)

              case "dataDistribution" => DataDistribution.dataDistribution(properties: Properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)

              case "crossComparison"=> CrossEnvComparison.crossComparison(properties: Properties, projectId.toInt, subProjectId.toInt, pipelineId.toInt, scenarioId.toInt, scenarioName, batchDate, batchSeqId.toInt, configDBName)
            }
          }
        }
        logger.info("Completing all validations")

        if (sourceType == "file" & scenarioIdList.contains(Constants.ID_SCENARIO_SI)) {
          val doneFileStat = sqlContext.sql("select additionalcomments from " + configDBName + Constants.DOT + Constants.TBL_SQUID_VS + " where project_id = '" + projectId + "' and sub_project_id = '" + subProjectId + "' and pipeline_id = '" + pipelineId + "' and scenario_id = '" + Constants.ID_SCENARIO_SI + "'  and batch_date = '" + batchDate + "' and batch_seq_id = '" + batchSeqId + "'").collect().map(_.getString(0)).mkString(Constants.EMPTY_STRING).split(Constants.SPACE)(0)
          if (doneFileStat == "inclusion") {
            val mailSubject = "SQUID Validation for the Project => " + source + " and BatchDate => " + batchDate
            QuestUtils.sendMail(properties, projectId, subProjectId, pipelineId, batchDate, batchSeqId, Constants.FROM, to_dl_list, cc_dl_list, bcc_dl_list, mailSubject, QuestUtils.createEmailBodySI(), sparkSession.emptyDataFrame, Constants.FAILURE)
          }
          QuestUtils.createDoneFile(config, source, frequency, batchDate, configDF, doneFileStat + ".done")
        }

        val distinctStatus = sqlContext.sql("select distinct status from " + configDBName + Constants.DOT + Constants.TBL_SQUID_VS + "  where project_id = '" + projectId + "' and sub_project_id = '" + subProjectId + "' and pipeline_id = '" + pipelineId + "'  and batch_date = '" + batchDate + "' and batch_seq_id = '" + batchSeqId + "'").map(r => r.getString(0)).collect().toList

        if (distinctStatus.contains("Abort") || distinctStatus.contains("Fail") || distinctStatus.isEmpty) {
          QuestUtils.createJobDoneFile(config, projectId, subProjectId, pipelineId, batchDate, batchSeqId, skiptool, properties, "fail.done")
        } else if (distinctStatus.length == 1 && distinctStatus.contains("Pass")) {
          QuestUtils.createJobDoneFile(config, projectId, subProjectId, pipelineId, batchDate, batchSeqId, skiptool, properties, "pass.done")
        }

        val rptCols = sparkSession.sql("select " + Constants.DQ_TREND_VS_COLS + Constants.COMMA + Constants.SQUID_VS_COLS + Constants.COMMA + Constants.QUERY_VS_COLS + " from " + configDBName + Constants.DOT + Constants.TBL_RPT_MD + " where " + Constants.PROJECT_ID + Constants.EQUALS + projectId + Constants.SPACE + Constants.LOGICAL_AND + Constants.SPACE + Constants.SUB_PROJECT_ID + Constants.EQUALS + subProjectId + Constants.SPACE + Constants.LOGICAL_AND + Constants.SPACE + Constants.PIPELINE_ID + Constants.EQUALS + pipelineId).collectAsList()
        val (dqTrendVSCols, squidVSCols, queryVSCols) = (rptCols.get(0)(0).toString, rptCols.get(0)(1).toString, rptCols.get(0)(2).toString)
        val squidVsDF = sparkSession.sql("select " + squidVSCols + " from " + configDBName + Constants.DOT + Constants.TBL_SQUID_VS + " where " + Constants.PROJECT_ID + " = '" + projectId + "' and " + Constants.SUB_PROJECT_ID + " = '" + subProjectId + "' and " + Constants.PIPELINE_ID + " = '" + pipelineId + "'  and " + Constants.BATCH_DATE + " = '" + batchDate + "' and " + Constants.BATCH_SEQUENCE_ID + " = '" + batchSeqId + "'")
        if (email_comm_reqd_flag == Constants.YES) {
          QuestUtils.sendMailAttachment(configDBName, properties, projectId, subProjectId, pipelineId, batchDate, batchSeqId, source, to_dl_list, cc_dl_list, bcc_dl_list, mail_attach_category, dqTrendVSCols, queryVSCols, squidVsDF)
        }
      }
      val duration = (System.nanoTime - t1) / 1e9d
      logger.info("duration of the job " + duration)
    }
    catch {
      case e: Exception => logger.error("Error in Main Class " + e.getMessage)
        e.printStackTrace()
        logger.info("Create Abort File")
        QuestUtils.createJobDoneFile(config, projectId, subProjectId, pipelineId, batchDate, batchSeqId, skiptool, properties, "abort.done")
        val mailSubject = "SQUID Failed for the Project => " + source + " and BatchDate => " + batchDate
        QuestUtils.sendMail(properties, projectId, subProjectId, pipelineId, batchDate, batchSeqId, Constants.FROM, bcc_dl_list, bcc_dl_list, bcc_dl_list, mailSubject, e.getMessage, sparkSession.emptyDataFrame, Constants.FAILURE)
        throw e
    } finally {
      sparkSession.stop()
      sc.stop()
    }
  }
}
