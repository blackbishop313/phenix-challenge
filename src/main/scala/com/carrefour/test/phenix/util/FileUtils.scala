package com.carrefour.test.phenix.util

import java.io.{FileOutputStream, PrintWriter}
import java.nio.file.{Files, Path, Paths}

import org.apache.log4j.Logger

import scala.io.Source

object FileUtils {

  final val log: Logger = Logger.getLogger(this.getClass)

  /**
    * Writes content to file
    * @param path path to file
    * @param content stream of strings
    */
  def writeToFile(path: Path, content: Stream[String]) : Unit = {
    val os = new PrintWriter(new FileOutputStream(path.toString))
    log.info(s"writing file to ${path.toAbsolutePath.toString}...")
    content.foreach(os.println)
    os.close()
    log.info(s"file successfully saved to ${path.toAbsolutePath.toString}")
  }

  /**
    * Write to multiple files at once
    * @param files Map of Path and Stream of strings
    */
  def writeToFiles(files: Map[Path, Stream[String]]): Unit = {
    files.par.foreach {
      file => writeToFile(file._1, file._2)
    }
  }

  /**
    * Read file content to stream from given path
    * @param path Path to file
    * @param charset encoding. by default it's UTF-8
    * @return Stream of strings
    */
  def readFromFile(path: Path, charset: Option[String]= Some("UTF-8")) : Stream[String] = {
    log.info(s"reading file content from ${path.toAbsolutePath.toString}...")
    val bufferedStream = charset match {
      case Some(enc) => Source.fromFile(path.toString, enc)
      case _ => Source.fromFile(path.toString, enc= "UTF-8")
    }
    log.info(s"file successfully loaded from ${path.toAbsolutePath.toString}")
    bufferedStream.getLines.toStream
  }

  /**
    * Checks if file path exists
    * @param path string path
    * @return Boolean
    */
  def fileExists(path: String): Boolean = {
    Files.exists(Paths.get(path))
  }



}
