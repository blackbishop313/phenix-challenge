package com.carrefour.test.phenix.task

import com.carrefour.test.phenix.model.GlobalConfig
import com.carrefour.test.phenix.util.{DateTimeUtils, LogUtils}
import org.apache.log4j.Logger

import scala.util.{Failure, Success}

/**
  * The Calculation Flow is the sequence of executions of the tasks TodayCalculation and WeekCalculation.
  * This class launches this sequence starting by the day calculation.
  */
object CalculationFlow {

  final val log: Logger = Logger.getLogger(this.getClass)

  def execute(): Unit = {

    log.info("Executing all calculation tasks...")
    val (attempt, startedAt, endedAt) = LogUtils.measureTime {
      executeTodayCalculation()
      executeWeekCalculation()
    }
    val elapsedTime = DateTimeUtils.getDiff(startedAt, endedAt)
    attempt match {
      case Success(_) => log.info(s"all tasks completed with success in $elapsedTime seconds")
      case Failure(exception) => log.error(exception.getMessage, exception)
    }
  }

  /**
    * Execute archiving task
    *
    * @return execution logs
    */
  def executeTodayCalculation() = {
    val (attempt, startedAt, endedAt) = LogUtils.measureTime {
      DayCalculationTask.execute(GlobalConfig.inputConfig)
    }
    val elapsedTime = DateTimeUtils.getDiff(startedAt, endedAt)
    attempt match {
      case Success(nb) => log.info(s"task completed in $elapsedTime seconds. $nb files successfully generated")
      case Failure(exception) => log.error(exception.getMessage, exception)
    }

  }

  /**
    * Execute Week Calculation task
    *
    * @return execution logs
    */
  def executeWeekCalculation() = {
    val (attempt, startedAt, endedAt) = LogUtils.measureTime {
      WeekCalculationTask.execute(GlobalConfig.inputConfig)
    }
    val elapsedTime = DateTimeUtils.getDiff(startedAt, endedAt)
    attempt match {
      case Success(nb) => log.info(s"task completed in $elapsedTime seconds. $nb files successfully generated")
      case Failure(exception) => log.error(exception.getMessage, exception)
    }

  }



}

