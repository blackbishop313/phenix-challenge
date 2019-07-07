package com.carrefour.test.phenix.importer

import com.carrefour.test.phenix.model.{Product, Transaction}

trait Parser [T] {
  def deserialize(line: Array[String]) : T
}


object TransactionParser  extends Parser[Transaction] {
  override def deserialize(line: Array[String]) : Transaction = {
    // val transactionDatetimePattern: String = "yyyyMMdd'T'HHmmss"
    line match {
      case Array(txId, _, storeUuid, productId, quantity) =>
        Transaction(txId.toInt,
          //DateTimeUtils.toDate(txDatetime, transactionDatetimePattern).get, //never used
          storeUuid,
          productId.toInt,
          quantity.toInt)
      case _ => throw new Exception(s"could not deserialize line ${line.toString} to Transaction object")
    }
  }
}

object ProductParser extends Parser[Product] {
  override def deserialize(line: Array[String]) : Product = {
    line match {
      case Array(productId, price) =>
        Product(productId.toInt, price.toDouble)
      case _ => throw new Exception(s"could not deserialize line ${line.toString} to Product object")
    }
  }
}
