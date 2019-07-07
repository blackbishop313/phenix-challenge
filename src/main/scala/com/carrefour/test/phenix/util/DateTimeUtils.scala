package com.carrefour.test.phenix.util

import java.text.SimpleDateFormat
import java.time.{LocalDate, ZoneId}
import java.util.{Calendar, Date, Locale, TimeZone}

import scala.util.{Failure, Success, Try}

object DateTimeUtils {


  /**
    * Returns current date
    */
  def currentDate(): Date = {
    Calendar.getInstance().getTime
  }

  /**
    * Returns a string containing the date from current date + delay
    *
    * @param delay  the delay from the current date; it can be positive (date in the future) or negative (date in the past)
    * @return
    */
  def currentDateSub(delay: Int): Date = {
    val cal = Calendar.getInstance()
    cal.add(Calendar.DAY_OF_YEAR, delay)

    cal.getTime
  }

  /**
    * Formats a date
    *
    * @param date    date to be formatted
    * @param pattern pattern
    * @return the date formatted using the pattern
    */
  def formatDate(date: Date, pattern: String): String = new SimpleDateFormat(pattern, Locale.US).format(date)


  /**
    * Converts a date in String format from a date pattern to a date
    *
    * @param value    a date in String format
    * @param pattern  date pattern
    * @param locale   date locale, optional
    * @return         parsed date
    */
  def toDate(value: String, pattern: String, locale: Option[String] = None): Option[Date] = {

    Try {
      val loc = locale match {
        case Some(l) =>  new Locale(l)
        case None => Locale.getDefault
      }
      val parser = new SimpleDateFormat(pattern, loc)
      parser.setTimeZone(TimeZone.getTimeZone("UTC"))
      parser.setLenient(false)
      parser.parse(value.trim)
    } match {
      case Success(d) => Some(d)
      case Failure(_) => None
    }
  }

  /**
    * Converts a date pattern to correspondent Regex
    * @param pattern date pattern
    * @return regex String
    */
  def toRegex(pattern: String): String = {
    pattern.replaceAll("[y|d|h|m|s|H]+", "[0-9]+")
      .replaceAll("M+", "[0-9A-Za-z]+")
  }

  /**
    * Generates a list of dates formatted with given pattern
    * @param pattern date pattern
    * @param delay the delay from the current date; it can be positive (date in the future) or negative (date in the past)
    * @return list of formatted dates
    */
  def formatDates(pattern: String, delay: Int): Seq[String] = {
    if (delay == 0)
      Seq(DateTimeUtils.formatDate(currentDate(), pattern))
    else
      (0 until delay).map {
        day =>
          val dayDate = DateTimeUtils.currentDateSub(-day)
          DateTimeUtils.formatDate(dayDate, pattern)
      }
  }

  /**
    * Returns the difference in seconds between two dates
    *
    * @param startDate start date
    * @param endDate   end date
    * @return the difference in seconds between start date and end date
    */
  def getDiff(startDate: Date, endDate: Date): Long = (endDate.getTime - startDate.getTime)/1000


  /**
    * Convert a date from Date to LocalDate
    * @param value date in Date format
    * @return LocalDate
    */
  def toLocalDate(value: Date): LocalDate = {
    value.toInstant
      .atZone(ZoneId.systemDefault)
      .toLocalDate
  }
}
