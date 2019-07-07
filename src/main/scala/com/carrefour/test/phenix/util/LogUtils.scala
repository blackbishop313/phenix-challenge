package com.carrefour.test.phenix.util

import java.util.Date

import scala.util.Try

object LogUtils {


    /**
      * Executes a code block and returns start and end times
      *
      * @param block block to be executed
      * @tparam T block returning type
      * @return the result of the block execution, the start time, the end time
      */
    def measureTime[T](block: => T): (Try[T], Date, Date) = {
      val startedAt = new Date
      val result = Try(block)
      val endedAt = new Date
      (result, startedAt, endedAt)
    }

}
