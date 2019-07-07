package com.carrefour.test.phenix.task

import com.carrefour.test.phenix.importer.{FlatFileImporter, TransactionParser}
import com.carrefour.test.phenix.model.FileTypes.FileTypes
import com.carrefour.test.phenix.model._
import com.carrefour.test.phenix.util.FileUtils

object DayCalculationTask extends CalculationTask[InputConfig] {

  override def taskName: String = "Calculating today aggregations"

  override def doCalculation(confObject: Map[FileTypes, InputConfig]): Int = {

    val transactionFileProperties = confObject(FileTypes.TRANSACTION).fileProperties
    val resultFileProperties = confObject(FileTypes.SALES_RESULT).fileProperties
    val transactions = ImportTodayTransactions(transactionFileProperties)
    val topSalesByStore = getTopSalesVolumeByStore(transactions)

    val result = topSalesByStore.map {
      sale =>
        val storeSalesVolume = sale.serialize(resultFileProperties.delimiter.toString)
        GlobalConfig.generateResultFile(MeasureTypes.VENTES, 0, Option(storeSalesVolume._1)) -> storeSalesVolume._2
    }.toMap

    FileUtils.writeToFiles(result)

    result.size
  }


  /**
    * Loads transaction files of the day
    * @param fp transaction file properties
    * @return sequence of stream TransactionRecords
    */
  private def ImportTodayTransactions(fp: FileProperties): Seq[Stream[TransactionRecords]] = {
    val ffi = FlatFileImporter[Transaction](fp, TransactionParser)
    val files = GlobalConfig.getInputFiles(FileTypes.TRANSACTION)

    if (files.isEmpty)
      log.error("no transaction data files found in the input data folder")
    else
      log.info(s"${files.size} transaction files found for today calculation")

    files.map{
      tf => tf.map {
        t => {
          val input = ffi.importFile(t._1)
          TransactionRecords(input, t._2)
        }
      }.toStream
    }
  }

  /**
    * Calculates top sales volume by store
    * @param trans sequence of stream TransactionRecords
    * @param top top sales. By default it's 100 best sales
    * @return sequence of StoreSalesVolume
    */
  private def getTopSalesVolumeByStore(trans: Seq[Stream[TransactionRecords]], top: Int = 100): Seq[StoreSalesVolume] = {
    val allSalesByStore = trans.flatMap {
      tsream =>
        tsream.flatMap {
          tr =>
            tr.input
              .groupBy(t => (t.storeUuid, t.productId))
              .mapValues(_.map(_.quantity).sum)
              .map(group =>
                (group._1._1, group._1._2, group._2))
              .groupBy(_._1)
              .map(store =>
                (store._1, store._2.map(prd => SalesVolume(prd._2, prd._3)).toStream))
        }
    }

    allSalesByStore.map(s =>
      StoreSalesVolume(s._1, s._2.sortWith(_.salesVolume > _.salesVolume).take(top))
    )
  }


}
