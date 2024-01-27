package io.github.blakrise.layers.common

object Converter {
  object IntConverter {
    /**
     * Converts ColVal into Integer 
     * 
     * @param colVal ColVal
     * @return
     */
    def apply(colVal: ColVal): Int = colVal.value.toInt
  }

  object LongConverter {
    /**
     * Converts ColVal into Long 
     *
     * @param colVal ColVal
     * @return
     */
    def apply(colVal: ColVal): Long = colVal.value.toLong
  }

  object FloatConverter {
    /**
     * Converts ColVal into Float
     *
     * @param colVal ColVal
     * @return
     */
    def apply(colVal: ColVal): Float = colVal.value.toFloat
  }

  object DoubleConverter {
    /**
     * Converts ColVal into Double
     *
     * @param colVal ColVal
     * @return
     */
    def apply(colVal: ColVal): Double = colVal.value.toDouble
  }

  object StringConverter {
    /**
     * Converts ColVal into String
     *
     * @param colVal ColVal
     * @return
     */
    def apply(colVal: ColVal): String = colVal.value
  }
}

