package com.carrefour.test.phenix.task

import com.carrefour.test.phenix.model.FileTypes.FileTypes
import org.apache.log4j.Logger


/**
  * Executes a task given a configuration object
  *
  * @tparam T class of the configuration object
  */

trait CalculationTask[T] {

    final val log: Logger = Logger.getLogger(this.getClass)

    def taskName: String


    def doCalculation(confObject: Map[FileTypes, T]): Int


    /**
      * Executes a task
      *
      * @param taskConf task configuration
      */
    def execute(taskConf: Map[FileTypes, T]): Int = {
      log.info(s"Starting $taskName...")

      val nbFiles = doCalculation(taskConf)

      log.info(s"$taskName finished")

      nbFiles
    }

}
