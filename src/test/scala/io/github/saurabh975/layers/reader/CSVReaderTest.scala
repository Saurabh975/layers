package io.github.saurabh975.layers.reader

import io.github.saurabh975.layers.base.BaseTest
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.AnalysisException

import java.io.File
import scala.reflect.io.Directory

class CSVReaderTest extends BaseTest{
  val testingDirectory = new Directory(new File(dir.path.concat("/csvDirectory")))
  val basePath = new Path(testingDirectory.path)

  override def beforeAll(): Unit = {
    testingDirectory.createDirectory()
    testingPaths(testingDirectory.path, ".csv")
  }

  override def afterAll(): Unit = {
    testingDirectory.deleteRecursively()
  }

  "CSVReader" should "read the csv dataframe and return the data read" in {
    val df = CSVReader.read(spark, testingDirectory.path)
    assert(df.count() == 7290)
  }

  "CSVReader" should "throw AnalysisException if base path provided does not exist" in {
    assertThrows[AnalysisException](CSVReader.read(spark, "basePath"))
  }
}
