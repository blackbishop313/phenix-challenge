package com.carrefour.test.phenix.task

import com.carrefour.test.phenix.model.GlobalConfig
import com.carrefour.test.phenix.util.FileUtils
import org.apache.log4j.Logger

import scala.util.control.NonFatal

/**
  * Launcher of an Calculation Flow.
  *
  * Tries to set global configuration from arguments
  * If success, launches a Calculation Flow, otherwise logs the exception.
  */

object CalculationLauncher {

    final val log: Logger = Logger.getLogger(this.getClass)

    case class CLIArgs(inputDataFolder: String = "",
                       outputResultFolder: String = "",
                       appName: String = "phenix-challenge")

    def main(args: Array[String]): Unit = {

      val parser = new scopt.OptionParser[CLIArgs]("phenix-challenge") {
        head("phenix-challenge", "1.0.0")

        opt[String]("input.data.folder").required
          .action((x, c) => c.copy(inputDataFolder = x))
          .validate{x =>
            if (FileUtils.fileExists(x)) success
            else failure("Value <input.data.folder> is not a valid path")}
          .text("input data folder")

        opt[String]("output.result.folder").required
          .action((x, c) => c.copy(outputResultFolder = x))
          .validate{x =>
            if (FileUtils.fileExists(x)) success
            else failure("Value <output.result.folder> is not a valid path")}
          .text("output result folder")
      }

      parser.parse(args, CLIArgs()).foreach { config =>
        try {
            GlobalConfig.setConfig(config)
            CalculationFlow.execute()
        } catch {
          case NonFatal(ex) =>
            log.error("error", ex)
            ex.printStackTrace()
        }

      }

    }

}
