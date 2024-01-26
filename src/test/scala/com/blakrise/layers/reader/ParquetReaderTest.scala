package com.blakrise.layers.reader

import com.blakrise.layers.base.BaseTest
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException

import java.io.File
import scala.reflect.io.Directory

class ParquetReaderTest extends BaseTest{
  val testingDirectory = new Directory(new File(dir.path.concat("/parquetDirectory")))
  val basePath = new Path(testingDirectory.path)

  override def beforeAll(): Unit = {
    testingDirectory.createDirectory()
    testingPaths(testingDirectory.path, ".parquet")
  }

  override def afterAll(): Unit = {
    testingDirectory.deleteRecursively()
  }

  "ParquetReader" should "read the parquet dataframe and return the data read" in {
    val df = ParquetReader.read(spark, testingDirectory.path)
    assert(df.count() == 7290)
  }

  "ParquetReader" should "throw AnalysisException if base path provided does not exist" in {
    assertThrows[AnalysisException](ParquetReader.read(spark, "basePath"))
  }
}