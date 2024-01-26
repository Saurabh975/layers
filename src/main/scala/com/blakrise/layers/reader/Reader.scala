package com.blakrise.layers.reader

import org.apache.spark.sql.{DataFrame, SparkSession}

sealed trait Reader{

  /**
   * A utility to read data through spark
   *
   * @param spark  This is the Spark session
   * @param options       This is a map of options
   * @param paths         This is the path(s) from which the file(s) need to be read
   * @return Dataframe: The dataframe after reading data from all the paths
   * */
  def read(spark: SparkSession, options: Map[String, String], paths: String*): DataFrame

  /**
   * A utility to read data through spark
   *
   * Similar to [[Reader.read()]] method but with empty options map
   * @param spark This is the Spark session
   * @param paths        This is the path(s) from which the file(s) need to be read
   * @return Dataframe: The dataframe after reading data from all the paths
   * */
  def read(spark: SparkSession, paths: String*): DataFrame
}

object ORCReader extends Reader {
  override def read(spark: SparkSession, options: Map[String, String], paths: String*): DataFrame =
    spark.read.options(options).orc(paths: _*)

  override def read(spark: SparkSession, paths: String*): DataFrame =
    ORCReader.read(spark, Map.empty[String, String], paths: _*)
}


object ParquetReader extends Reader {
  override def read(spark: SparkSession, options: Map[String, String], paths: String*): DataFrame = spark.read
    .options(options).parquet(paths: _*)

  override def read(spark: SparkSession, paths: String*): DataFrame =
    ParquetReader.read(spark, Map.empty[String, String], paths: _*)
}


object JSONReader extends Reader {
  override def read(spark: SparkSession, options: Map[String, String], paths: String*): DataFrame = spark.read
    .options(options).json(paths: _*)

  override def read(spark: SparkSession, paths: String*): DataFrame =
    JSONReader.read(spark, Map.empty[String, String], paths: _*)
}


object CSVReader extends Reader {
  override def read(spark: SparkSession, options: Map[String, String], paths: String*): DataFrame = spark.read
    .options(options).csv(paths: _*)

  override def read(spark: SparkSession, paths: String*): DataFrame =
    CSVReader.read(spark, Map.empty[String, String], paths: _*)
}


