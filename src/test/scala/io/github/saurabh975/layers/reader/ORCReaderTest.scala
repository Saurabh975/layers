package io.github.saurabh975.layers.reader

import io.github.saurabh975.layers.base.BaseTest
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException

import java.io.File
import scala.reflect.io.Directory

class ORCReaderTest extends BaseTest{
  val testingDirectory = new Directory(new File(dir.path.concat("/orcDirectory")))
  val basePath = new Path(testingDirectory.path)

  override def beforeAll(): Unit = {
    testingDirectory.createDirectory()
    testingPaths(testingDirectory.path, ".orc")
  }

  override def afterAll(): Unit = {
    testingDirectory.deleteRecursively()
  }

  "ORCReader" should "read the orc dataframe and return the data read" in {
    val df = ORCReader.read(spark, testingDirectory.path)
    assert(df.count() == 7290)
  }

  "ORCReader" should "throw AnalysisException if base path provided does not exist" in {
    assertThrows[AnalysisException](ORCReader.read(spark, "basePath"))
  }
}
