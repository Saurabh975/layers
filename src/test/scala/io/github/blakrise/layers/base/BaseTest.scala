package io.github.blakrise.layers.base

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import scala.reflect.io.Directory
import scala.util.Random

case class TestData(id: Int, data: String)

abstract class BaseTest extends AnyFlatSpec with BeforeAndAfterAll {

  lazy val spark: SparkSession = SparkSession.builder().appName("Test")
    .config("spark.master", "local[2]").getOrCreate()
  spark.sqlContext.setConf("spark.sql.shuffle.partitions", "1")

  protected val dir = new Directory(new File("src/test/resources"))

  protected def testingPaths(dirPath: String, fileExtension: String): Unit = {
    // Long Partition
    val longPartnVal: List[Long] = List(2023110518230000L, 2023110518240000L, 2023110718240000L)

    val longPartn: List[String] = longPartnVal map (partn =>
      dirPath.concat(s"/long_partn_col=$partn"))

    // Int Partition
    val intPartnVal: List[Int] = List(123, 234, 345)

    val intPartn: List[String] = intPartnVal flatMap (partn =>
      longPartn map (path => path.concat(s"/int_partn_col=$partn")))

    //Float Partition
    val floatPartnVal: List[Float] = List(1.0F, 2.1F, 3.2F)

    val floatPartn: List[String] = floatPartnVal flatMap (partn =>
      intPartn map (path => path.concat(s"/float_partn_col=$partn")))

    // Double Partition
    val doublePartnVal: List[Double] = List(123.5, 234.6, 345.7)

    val doublePartn: List[String] = doublePartnVal flatMap { partn =>
      floatPartn map { path =>
        path.concat(s"/double_partn_col=$partn")
      }
    }
    // String partition
    val stringPartnVal: List[String] = List("val1", "val2", "val3")

    val directories: List[Directory] = stringPartnVal flatMap { partn =>
      doublePartn map { path =>
        new Directory(new File(path.concat(s"/string_partn_col=$partn")))
      }
    }
    directories foreach (_.createDirectory())

    //leaf Files
    import spark.implicits._
    val sampleDataset: Dataset[TestData] = spark.range(30)
      .map(id => TestData(id.toInt, s"data$id"))
    val dfToWrite = sampleDataset.toDF
    fileExtension match {
      case ".orc" => directories.foreach(directory =>
        dfToWrite.coalesce(1).write.mode(SaveMode.Overwrite).orc(directory.path))
      case ".parquet" => directories.foreach(directory =>
        dfToWrite.coalesce(1).write.mode(SaveMode.Overwrite).parquet(directory.path))
      case ".json" => directories.foreach(directory =>
        dfToWrite.coalesce(1).write.mode(SaveMode.Overwrite).json(directory.path))
      case ".txt" => directories.foreach(directory =>
        dfToWrite.coalesce(1).write.mode(SaveMode.Overwrite).text(directory.path))
      case ".csv" => directories.foreach(directory =>
        dfToWrite.coalesce(1).write.mode(SaveMode.Overwrite).csv(directory.path))
    }
  }
}
