package com.carrefour.test.phenix.model


sealed trait Result extends scala.Product {
  def serialize(delimiter: String): String = this.productIterator.mkString(delimiter)
}

abstract class StoreResult (storeUuid: String, sales: Stream[Result]) {
  def serialize(delimiter: String): (String, Stream[String]) = {
    storeUuid -> sales.map(_.serialize(delimiter))
  }
}


case class SalesVolume(productId: Int, salesVolume: Int) extends Result
case class SalesAmount(productId: Int, salesAmount: Double) extends Result


case class StoreSalesVolume(storeUuid: String, sales: Stream[SalesVolume]) extends StoreResult(storeUuid, sales)
case class StoreSalesAmount(storeUuid: String, sales: Stream[SalesAmount]) extends StoreResult(storeUuid, sales)


