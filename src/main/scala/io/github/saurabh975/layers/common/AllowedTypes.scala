package io.github.saurabh975.layers.common

/**
 * Trait to restrict user into providing only Int, Long, Float or Double type values
 */
sealed trait AllowedNumericTypes[T]

/**
 * Object to restrict user into providing only Int, Long, Float or Double type values
 */
object AllowedNumericTypes {
  implicit object LongAllowed extends AllowedNumericTypes[Long]

  implicit object DoubleAllowed extends AllowedNumericTypes[Double]

  implicit object IntAllowed extends AllowedNumericTypes[Int]

  implicit object FloatAllowed extends AllowedNumericTypes[Float]
}

/**
 * Trait to restrict user into providing only Int, Long, Float, Double or String type values
 */
sealed trait AllowedAllTypes[T]

/**
 * Object to restrict user into providing only Int, Long, Float, Double or String type values
 */
object AllowedAllTypes {
  implicit object LongAllowed extends AllowedAllTypes[Long]

  implicit object DoubleAllowed extends AllowedAllTypes[Double]

  implicit object IntAllowed extends AllowedAllTypes[Int]

  implicit object FloatAllowed extends AllowedAllTypes[Float]

  implicit object StringAllowed extends AllowedAllTypes[String]
}
