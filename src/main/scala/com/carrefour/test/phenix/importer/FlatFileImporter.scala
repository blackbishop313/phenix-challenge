package com.carrefour.test.phenix.importer

import java.nio.file.Path

import com.carrefour.test.phenix.model.FileProperties
import com.carrefour.test.phenix.util.FileUtils

case class FlatFileImporter[T](properties: FileProperties,
                               parser: Parser[T]) extends FileImporter[T] {

  final val DEFAULT_DELIMITER_CHAR: Char = ','
  final val DEFAULT_HAS_HEADER : Boolean = true
  final val DEFAULT_QUOTE: Char = '"'
  final val DEFAULT_ESCAPE: Char = '\\'
  final val DEFAULT_CHARSET: String = "UTF-8"

  override def importFiles(files: Seq[Path]): Stream[T] = {
    files.toStream
      .flatMap{
        file => importFile(file)
      }
  }

  override def importFile(file: Path): Stream[T] = {
    val lines = FileUtils.readFromFile(file, Some(properties.charset))
    val lineSkip = if (properties.hasHeader) {
      lines.drop(1)
    } else {
      lines
    }

    lineSkip.map{
        l =>
          val parsedLine = l.split(properties.delimiter)
          parser.deserialize(parsedLine)
      }
  }

}
