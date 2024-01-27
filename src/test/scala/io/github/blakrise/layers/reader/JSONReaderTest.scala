package io.github.blakrise.layers.reader

import io.github.blakrise.layers.base.BaseTest
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException

import java.io.File
import scala.reflect.io.Directory

class JSONReaderTest extends BaseTest{
  val testingDirectory = new Directory(new File(dir.path.concat("/jsonDirectory")))
  val basePath = new Path(testingDirectory.path)

  override def beforeAll(): Unit = {
    testingDirectory.createDirectory()
    testingPaths(testingDirectory.path, ".json")
  }

  override def afterAll(): Unit = {
    testingDirectory.deleteRecursively()
  }

  "JSONReader" should "read the json dataframe and return the data read" in {
    val df = JSONReader.read(spark, testingDirectory.path)
    assert(df.count() == 7290)
  }

  "JSONReader" should "throw AnalysisException if base path provided does not exist" in {
    assertThrows[AnalysisException](JSONReader.read(spark, "basePath"))
  }
}
