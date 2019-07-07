package com.carrefour.test.phenix.importer

import com.carrefour.test.phenix.model.{Product, Transaction}
import org.scalatest.{FlatSpec, Matchers}

class ParserTest extends FlatSpec with Matchers {

  "The TransactionParser" should "should deserialize correctly Transaction" in {
    val transactionLine =  "7478|20170514T130958+0200|dd43720c-be43-41b6-bc4a-ac4beabd0d9b|169|5"
    val transaction = TransactionParser.deserialize(transactionLine.split('|'))
    val deserializedObject = Transaction(7478, "dd43720c-be43-41b6-bc4a-ac4beabd0d9b", 169, 5)

    assert(transaction.storeUuid == deserializedObject.storeUuid &&
    transaction.productId == deserializedObject.productId &&
    transaction.txId == deserializedObject.txId &&
    transaction.quantity == deserializedObject.quantity)
  }


  "The ProductParser" should "should deserialize correctly Product" in {
    val productLine =  "1|15.44"
    val product = ProductParser.deserialize(productLine.split('|'))
    val deserializedObject = Product(1, 15.44)

    assert(product.productId == deserializedObject.productId &&
      product.price == deserializedObject.price)
  }

}
