package com.carrefour.test.phenix.model

import com.fasterxml.jackson.annotation.JsonProperty


case class InputConfig(@JsonProperty(value = "fileType", required = true)  fileType: String,
                       @JsonProperty(value = "fileNamePattern", required = true) fileNamePattern: String,
                       @JsonProperty(value = "fileDatePattern", required = true) fileDatePattern: String,
                       @JsonProperty(value = "fileProperties", required = true) fileProperties: FileProperties
                      )

case class FileProperties(@JsonProperty(value = "delimiter", required = false) delimiter: Char,
                          @JsonProperty(value = "hasHeader", required = false) hasHeader: Boolean,
                          @JsonProperty(value = "quote", required = false) quote: Char,
                          @JsonProperty(value = "escape", required = false) escape: Char,
                          @JsonProperty(value = "charset", required = false) charset: String)



object FileTypes extends Enumeration {
  type FileTypes = Value

  val TRANSACTION = Value("Transaction")
  val PRODUCT = Value("Product")
  val SALES_RESULT = Value("SalesResult")
}


object MeasureTypes extends Enumeration {
  type MeasureTypes = Value

  val CA = Value("ca")
  val VENTES = Value("ventes")
}

object FileVars extends Enumeration {
  type FileVars = Value

  val FILE_DATE_VAR = Value("${file_date}")
  val STORE_UUID_VAR = Value("${store_uuid}")
  val AGGREGATION_LEVEL_VAR = Value("${aggregation_level}")
  val MEASURE_TYPE_VAR = Value("${measure_type}")
  val DELTA_VAR = Value("${delta}")
}



