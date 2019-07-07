package com.carrefour.test.phenix.task

import com.carrefour.test.phenix.importer.{FlatFileImporter, ProductParser, TransactionParser}
import com.carrefour.test.phenix.model.FileTypes.FileTypes
import com.carrefour.test.phenix.model.{FileProperties, FileTypes, GlobalConfig, InputConfig, MeasureTypes, Product, ProductRecords, SQLLike, SalesAmount, StoreSalesAmount, Transaction, TransactionProductJoin, TransactionRecords}
import com.carrefour.test.phenix.util.FileUtils

object WeekCalculationTask extends CalculationTask[InputConfig] {

  override def taskName: String = "Calculating week aggregations"

  override def doCalculation(confObject: Map[FileTypes, InputConfig]): Int = {
    val (transactionFileProperties, productFileProperties, resultFileProperties) = (
      confObject(FileTypes.TRANSACTION).fileProperties,
      confObject(FileTypes.PRODUCT).fileProperties,
      confObject(FileTypes.SALES_RESULT).fileProperties
    )

    val (transactions, products) = ImportWeekFiles(transactionFileProperties, productFileProperties)
    val topSalesByStore = getTopSalesAmountByStore(transactions, products)
    val result = topSalesByStore.map {
      sale =>
        val storeSalesAmount = sale.serialize(resultFileProperties.delimiter.toString)
        GlobalConfig.generateResultFile(MeasureTypes.CA, 7, Option(storeSalesAmount._1)) -> storeSalesAmount._2
    }.toMap

    FileUtils.writeToFiles(result)

    result.size
  }


  /**
    * Loads transaction and product files of the the week (last 7 days starting by today)
    *
    * @param txFp transaction file properties
    * @param pFp  product file properties
    * @return sequence of stream TransactionRecords and sequence of stream ProductRecords
    */
  private def ImportWeekFiles(txFp: FileProperties, pFp: FileProperties): (Stream[TransactionRecords], Stream[ProductRecords]) = {
    val (transactionFFImporter, productFFImporter) = (FlatFileImporter[Transaction](txFp, TransactionParser),
      FlatFileImporter[Product](pFp, ProductParser)
    )
    val (txFiles, prdFiles) = (GlobalConfig.getInputFiles(FileTypes.TRANSACTION, 7),
      GlobalConfig.getInputFiles(FileTypes.PRODUCT, 7)
    )

    (txFiles.isEmpty, prdFiles.isEmpty) match {
      case (true, false) => log.error("no transaction data files found in the input data folder")
      case (false, true) => log.error("no product data files found in the input data folder")
      case (true, true) => log.error("no product nor transaction data files found in the input data folder")
      case (false, false) => log.info(s"${txFiles.size} transactions and ${prdFiles.size} product files found in the last 7 days")
    }

    val txRecords = txFiles.flatMap {
      tf =>
        tf.map {
          t => {
            val input = transactionFFImporter.importFile(t._1)
            TransactionRecords(input, t._2)
          }
        }
    }

    val prdRecords = prdFiles.flatMap {
      prdf =>
        prdf.map {
          p => {
            val input = productFFImporter.importFile(p._1)
            ProductRecords(input, p._2)
          }
        }
    }

    (txRecords.toStream, prdRecords.toStream)
  }

  /**
    * Calculates top sales amount by store
    *
    * @param txRecords  transaction records
    * @param prdRecords product records
    * @param top        top sales. By default it's 100 best sales
    * @return sequence of StoreSalesAmount
    */
  private def getTopSalesAmountByStore(txRecords: Stream[TransactionRecords], prdRecords: Stream[ProductRecords], top: Int = 100): Seq[StoreSalesAmount] = {
    val joinedTransactionProduct: Seq[TransactionProductJoin] = SQLLike.innerJoin(txRecords, prdRecords)

    val allSalesAmountByStore = joinedTransactionProduct.map {
      trj => (trj.tx, trj.price)
    }.groupBy(tx => (tx._1.storeUuid, tx._1.productId))
      .mapValues(_.map { t => t._1.quantity * t._2 }.sum)
      .map { group =>
        (group._1._1, group._1._2, group._2)
      }
      .groupBy(_._1)
      .map(store =>
        (store._1, store._2.map(prd => SalesAmount(prd._2, prd._3)).toStream)
      )

    allSalesAmountByStore.map(s =>
      StoreSalesAmount(s._1, s._2.sortWith(_.salesAmount > _.salesAmount).take(top))
    ).toSeq
  }
}
