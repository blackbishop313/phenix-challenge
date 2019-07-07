package com.carrefour.test.phenix.model

import java.nio.file.{Path, Paths}

import com.carrefour.test.phenix.task.CalculationLauncher.CLIArgs
import com.carrefour.test.phenix.util.DateTimeUtils
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.carrefour.test.phenix.model.FileTypes.FileTypes
import com.carrefour.test.phenix.model.MeasureTypes.MeasureTypes
import com.fasterxml.jackson.core.JsonParser.Feature
import org.apache.log4j.Logger


object GlobalConfig {

  final val log: Logger = Logger.getLogger(this.getClass)
  var inputConfig: Map[FileTypes, InputConfig] = _


  final private val fileDateVar = FileVars.FILE_DATE_VAR
  final private val storeUuidVar = FileVars.STORE_UUID_VAR
  final private val aggregationLevelVar = FileVars.AGGREGATION_LEVEL_VAR
  final private val measureTypeVar = FileVars.MEASURE_TYPE_VAR
  final private val deltaVar = FileVars.DELTA_VAR

  private var inputDataFolderPath: Path = _
  private var outputResultFolderPath: Path = _


  /**
    * Sets global config from arguments
    *
    * @param config arguments
    */
  def setConfig(config: CLIArgs): Unit = {

    inputDataFolderPath = Paths.get(config.inputDataFolder)
    outputResultFolderPath = Paths.get(config.outputResultFolder)
    inputConfig = getInputConfigs

    log.info("[phenix-challenge] inputDataFolder = " + inputDataFolderPath)
    log.info("[phenix-challenge] outputResultFolder = " + outputResultFolderPath)
    log.info("[phenix-challenge] inputConfig = " + inputConfig)

  }

  /**
    * Retrieves input data file for the period of calculation (delta)
    *
    * @param fileType either 'Transaction' or 'Product"
    * @param delta    period of calculation in days. By default it's 0 (current day)
    * @return List of paths and variables extracted from path
    */
  def getInputFiles(fileType: FileTypes, delta: Int = 0): Seq[Map[Path, PathVariables]] = {

    val fileDatePattern = inputConfig(fileType).fileDatePattern
    val deltaDates = DateTimeUtils.formatDates(fileDatePattern, delta)
    val inputFolderFiles = inputDataFolderPath.toFile.listFiles().filter(_.isFile)

    inputFolderFiles.filter { f =>
      transactionRegex.findFirstIn(f.getName).isDefined && fileType == FileTypes.TRANSACTION ||
        productRegex.findFirstIn(f.getName).isDefined && fileType == FileTypes.PRODUCT
    }
      .map { file =>
        file.getName match {
          case transactionRegex(_, fd, _) if deltaDates.contains(fd) =>
            val fdate = DateTimeUtils.toLocalDate(DateTimeUtils.toDate(fd, fileDatePattern).get)
            Map(file.toPath -> PathVariables(fdate, None))
          case productRegex(_, uuid, _, fd, _) if deltaDates.contains(fd) =>
            val otherVars = Option(Map("StoreUuid" -> uuid))
            val fdate = DateTimeUtils.toLocalDate(DateTimeUtils.toDate(fd, fileDatePattern).get)
            Map(file.toPath -> PathVariables(fdate, otherVars))
          case _ => Map[Path, PathVariables]()
        }
      }
      .filter(_.nonEmpty)
  }


  /**
    * Generates a result file path depending on measureType we calculate
    *
    * @param measureType type of aggregation, it can be either 'ca' or 'ventes'
    * @param delta       delta period of calculation in days, should be positive
    * @param storeUuid   the store uuid. If empty then we generate 'GLOBAL' file result
    * @return Path for the output result file
    */
  def generateResultFile(measureType: MeasureTypes, delta: Int = 0, storeUuid: Option[String] = None): Path = {
    val conf = inputConfig(FileTypes.SALES_RESULT)
    val (outputPattern, outputFileDatePattern) = (conf.fileNamePattern, conf.fileDatePattern)
    val fileDate = DateTimeUtils.formatDate(DateTimeUtils.currentDate(), outputFileDatePattern)
    val deltaFile = if (delta > 0) s"-${delta}J" else ""

    val outFile = {
      val outputPattern_ = outputPattern.replace(fileDateVar.toString, fileDate)
        .replace(measureTypeVar.toString, measureType.toString)
        .replace(deltaVar.toString, deltaFile)
      storeUuid match {
        case Some(uuid) =>
          outputPattern_.replace(aggregationLevelVar.toString, uuid)
        case _ => outputPattern_.replace(aggregationLevelVar.toString, "GLOBAL")
      }
    }

    Paths.get(outputResultFolderPath.toString, outFile)
  }


  /**
    * Reads configs from YAML file resource
    * Expected fileType in resource file : 'Transaction', 'Product', 'SalesResult'
    *
    * @return Map[fileType -> InputConfig)
    */
  private def getInputConfigs: Map[FileTypes, InputConfig] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.configure(Feature.AUTO_CLOSE_SOURCE, true)
    val inputStream = getClass.getResourceAsStream("/resources/configs.yaml")
    mapper.registerModule(DefaultScalaModule)
    val input = mapper.readValue(inputStream, classOf[Array[InputConfig]])

    inputStream.close()

    input.map(i => FileTypes.withName(i.fileType) -> i).toMap

  }

  /**
    * Builds file pattern regex depending on input file properties
    *
    * @param fileType either 'Transaction' or 'Product'
    * @return return regexp string
    */
  private def getFilePatternRegex(fileType: FileTypes): String = {
    val conf = inputConfig.getOrElse(fileType,
      throw new Exception(s"config not found for '$fileType'. check configs.yaml resource"))

    val (fileNamePattern, fileDatePattern) = (conf.fileNamePattern, conf.fileDatePattern)

    fileType match {
      case FileTypes.TRANSACTION =>
        val patternRegex = {
          fileNamePattern.replace(fileDateVar.toString, DateTimeUtils.toRegex(fileDatePattern))
        }
        """^""" + patternRegex + """$"""
      case FileTypes.PRODUCT =>
        val uuidRegex = "[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}"
        val patternRegex = fileNamePattern.replace(storeUuidVar.toString, uuidRegex)
          .replace(fileDateVar.toString, DateTimeUtils.toRegex(fileDatePattern))
        """^""" + patternRegex + """$"""
      case _ => ""
    }
  }

  lazy private val productRegex = getFilePatternRegex(FileTypes.PRODUCT).r
  lazy private val transactionRegex = getFilePatternRegex(FileTypes.TRANSACTION).r

}

