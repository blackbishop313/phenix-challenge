package com.carrefour.test.phenix.importer

import java.nio.file.Path
import org.apache.log4j.Logger


/**
  * Importer for flat files
  */

trait FileImporter[T] {

    final val log: Logger = Logger.getLogger(this.getClass)

    def importFiles(files: Seq[Path]): Stream[T]

    def importFile(file: Path): Stream[T]
}


