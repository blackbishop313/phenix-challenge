package com.carrefour.test.phenix.model

import java.time.LocalDate

//case class Transaction(txId: Int, txDatetime: Date, storeUuid: String, productId: Int, quantity: Int)

case class Transaction(txId: Int, storeUuid: String, productId: Int, quantity: Int)

case class Product(productId: Int, price: Double)

case class PathVariables(fileDate: LocalDate, otherVars: Option[Map[String, String]])

sealed trait Record[T] {
  def input: Stream[T]

  def vars: PathVariables

}

case class TransactionRecords(override val input: Stream[Transaction], override val vars: PathVariables
                             ) extends Record[Transaction]

case class ProductRecords(override val input: Stream[Product], override val vars: PathVariables
                         ) extends Record[Product]

case class TransactionProductJoin(tx: Transaction, price: Double)


object SQLLike {

  /**
    * Joins TransactionRecords with Product Records
    * On fileDate = fileDate dans storeUuid = storeUuid
    *
    * @param left  stream of TransactionRecords
    * @param right stream of ProductRecords
    * @return stream of TransactionProductJoin
    */
  def innerJoin(left: Stream[TransactionRecords], right: Stream[ProductRecords]): Stream[TransactionProductJoin] = {

    val days = left.map { tr => tr.vars.fileDate }.toList

    days.par.flatMap {
      day =>
        val dayTransaction = left.filter(_.vars.fileDate == day)
        val dayProducts: Seq[ProductRecords] = right.filter(_.vars.fileDate == day)

        dayTransaction.flatMap {
          tx =>
            tx.input.map {
              i =>
                TransactionProductJoin(i, getProductPrice(i.storeUuid, i.productId, dayProducts))
            }
        }
    }.toStream
  }

  /**
    * gets product prince given set of product files
    *
    * @param storeUuid store uuid
    * @param productId productId
    * @param products  sequence of ProductRecords
    * @return price if found otherwise 0.0
    */
  private def getProductPrice(storeUuid: String, productId: Int, products: Seq[ProductRecords]): Double = {
    val product = products.filter {
      prdRecord =>
        val othersVars = prdRecord.vars.otherVars
        othersVars match {
          case Some(vars) => vars.getOrElse("StoreUuid", "") == storeUuid
          case _ => false
        }
    }.flatMap(_.input)
      .map(p => p)
      .find(_.productId == productId)

    product match {
      case Some(p) => p.price
      case _ => 0.toDouble
    }
  }

}