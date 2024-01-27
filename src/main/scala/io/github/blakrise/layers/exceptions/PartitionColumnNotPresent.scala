package io.github.blakrise.layers.exceptions

import org.apache.spark.sql.AnalysisException

/**
 * If The Partition Column Provided by user is not found, this error is thrown
 * @param message Error message
 */
case class PartitionColumnNotPresent (override val message: String) extends AnalysisException(message)
