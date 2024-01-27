package io.github.saurabh975.layers.common

/**
 * Filters object which has implementation for <, <=, >, >=, between, ==, !=, in.
 * <, <=, >, >=, between are valid only for Int, Long, Float, Double and ==, != and in are
 * valid for Int, Long, Float, Double and String
 */
private object Filters {
  def lt[T: AllowedNumericTypes](lhs: ColVal, rhs: T): Boolean = {
    rhs match {
      case rhs: Int => Converter.IntConverter(lhs) < rhs
      case rhs: Long => Converter.LongConverter(lhs) < rhs
      case rhs: Double => Converter.DoubleConverter(lhs) < rhs
      case rhs: Float => Converter.FloatConverter(lhs) < rhs
    }
  }

  def lteq[T: AllowedNumericTypes](lhs: ColVal, rhs: T): Boolean = {
    rhs match {
      case rhs: Int => Converter.IntConverter(lhs) <= rhs
      case rhs: Long => Converter.LongConverter(lhs) <= rhs
      case rhs: Double => Converter.DoubleConverter(lhs) <= rhs
      case rhs: Float => Converter.FloatConverter(lhs) <= rhs
    }
  }

  def gt[T: AllowedNumericTypes](lhs: ColVal, rhs: T): Boolean = {
    rhs match {
      case rhs: Int => Converter.IntConverter(lhs) > rhs
      case rhs: Long => Converter.LongConverter(lhs) > rhs
      case rhs: Double => Converter.DoubleConverter(lhs) > rhs
      case rhs: Float => Converter.FloatConverter(lhs) > rhs
    }
  }

  def gteq[T: AllowedNumericTypes](lhs: ColVal, rhs: T): Boolean = {
    rhs match {
      case rhs: Int => Converter.IntConverter(lhs) >= rhs
      case rhs: Long => Converter.LongConverter(lhs) >= rhs
      case rhs: Double => Converter.DoubleConverter(lhs) >= rhs
      case rhs: Float => Converter.FloatConverter(lhs) >= rhs
    }
  }

  def bw[T: AllowedNumericTypes](lhs: ColVal, rhs: (T, T)): Boolean = {
    gteq(lhs, rhs._1) && lteq(lhs, rhs._2)
  }

  def eq[T: AllowedAllTypes](lhs: ColVal, rhs: T): Boolean = {
    rhs match {
      case rhs: Int => Converter.IntConverter(lhs) == rhs
      case rhs: Long => Converter.LongConverter(lhs) == rhs
      case rhs: Double => Converter.DoubleConverter(lhs) == rhs
      case rhs: Float => Converter.FloatConverter(lhs) == rhs
      case rhs: String => Converter.StringConverter(lhs) equals rhs
    }
  }

  def in[T: AllowedAllTypes](lhs: ColVal, rhs: Set[T]): Boolean = {

    rhs match {
      case stringSet: Set[String] if rhs.head.getClass.getName equals "java.lang.String" =>
        stringSet.contains(Converter.StringConverter(lhs))
      case intSet: Set[Int] if rhs.head.getClass.getName equals "java.lang.Integer" =>
        intSet.contains(Converter.IntConverter(lhs))
      case longSet: Set[Long] if rhs.head.getClass.getName equals "java.lang.Long" =>
        longSet.contains(Converter.LongConverter(lhs))
      case floatSet: Set[Float] if rhs.head.getClass.getName equals "java.lang.Float" =>
        floatSet.contains(Converter.FloatConverter(lhs))
      case doubleSet: Set[Double] if rhs.head.getClass.getName equals "java.lang.Double" =>
        doubleSet.contains(Converter.DoubleConverter(lhs))
    }
  }
}

/**
 * Predicate trait to create list of predicate. Has Implementation for operators like
 * <, <=, >, >=, =, !=, between and in
 */
sealed trait Predicate {
  def apply(colVal: ColVal): Boolean
}

object Predicate {

  /**
   * To create < predicate expression
   * @param value value to compare < with. It accepts only Int, Long, Float, Double
   * @tparam T Restricted for Numeric Types - Int, Long, Float, Double
   */
  case class <[T: AllowedNumericTypes](value: T) extends Predicate {
    override def apply(colVal: ColVal): Boolean = Filters.lt(colVal, value)
  }

  /**
   * To create <= predicate expression
   *
   * @param value value to compare <= with. It accepts only Int, Long, Float, Double
   * @tparam T Restricted for Numeric Types - Int, Long, Float, Double
   */
  case class <=[T: AllowedNumericTypes](value: T) extends Predicate {
    override def apply(colVal: ColVal): Boolean = Filters.lteq(colVal, value)
  }

  /**
   * To create > predicate expression
   *
   * @param value value to compare > with. It accepts only Int, Long, Float, Double
   * @tparam T Restricted for Numeric Types - Int, Long, Float, Double
   */
  case class >[T: AllowedNumericTypes](value: T) extends Predicate {
    override def apply(colVal: ColVal): Boolean = Filters.gt(colVal, value)
  }

  /**
   * To create > predicate expression
   *
   * @param value value to compare >= with. It accepts only Int, Long, Float, Double
   * @tparam T Restricted for Numeric Types - Int, Long, Float, Double
   */
  case class >=[T: AllowedNumericTypes](value: T) extends Predicate {
    override def apply(colVal: ColVal): Boolean = Filters.gteq(colVal, value)
  }

  /**
   * To create between predicate expression
   *
   * @param leftBound left bound of between
   * @param rightBound right bound of between
   * @tparam T Restricted for Numeric Types - Int, Long, Float, Double
   */
  case class between[T: AllowedNumericTypes](leftBound: T, rightBound: T) extends Predicate {
    override def apply(colVal: ColVal): Boolean = Filters.bw(colVal, (leftBound, rightBound))
  }

  /**
   * To create equal predicate expression
   *
   * @param value value to compare == with. It accepts Int, Long, Float, Double and String
   * @tparam U Valid Types - Int, Long, Float, Double and String
   */
  case class equal[U: AllowedAllTypes](value: U) extends Predicate {
    override def apply(colVal: ColVal): Boolean = Filters.eq(colVal, value)
  }

  /**
   * To create notEqual predicate expression
   *
   * @param value value to compare != with. It accepts Int, Long, Float, Double and String
   * @tparam U Valid Types - Int, Long, Float, Double and String
   */
  case class notEqual[U: AllowedAllTypes](value: U) extends Predicate {
    override def apply(colVal: ColVal): Boolean = !Filters.eq(colVal, value)
  }

  /**
   * To create in predicate expression
   *
   * @param values Seq of values to to check if the argument falls in the values list.
   *               It accepts Int, Long, Float, Double and String
   * @tparam U Valid Types - Int, Long, Float, Double and String
   */
  case class in[U: AllowedAllTypes](values: U*) extends Predicate {
    def apply(colVal: ColVal): Boolean = Filters.in(colVal, values.toSet)
  }
}