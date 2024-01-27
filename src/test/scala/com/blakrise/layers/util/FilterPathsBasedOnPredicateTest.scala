package com.blakrise.layers.util

import com.blakrise.layers.common.Predicate.{<, _}
import com.blakrise.layers.base.BaseTest
import com.blakrise.layers.common.{Column, Predicate}
import com.blakrise.layers.exceptions.PartitionColumnNotPresent
import com.blakrise.layers.reader.ORCReader
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._

import java.io.{File, FileNotFoundException}
import scala.reflect.io.Directory

class FilterPathsBasedOnPredicateTest extends BaseTest {
  val testingDirectory = new Directory(new File(dir.path.concat("/testingDirectory")))
  val basePath = new Path(testingDirectory.path)

  override def beforeAll(): Unit = {
    testingDirectory.createDirectory()
    testingPaths(testingDirectory.path, ".orc")
  }

  override def afterAll(): Unit = {
    testingDirectory.deleteRecursively()
  }

  "filter" should "return list of paths after applying predicates" in {
    val predicates: Map[Column, List[Predicate]] = Map(
      Column("string_partn_col") -> List(in("val1", "val2")),
      Column("double_partn_col") -> List(>(123.0), <(235.0)),
      Column("float_partn_col") -> List(between(1.0F, 4.0F)),
      Column("int_partn_col") -> List(equal(234)),
      Column("long_partn_col") -> List(>=(2023110518230000L), <=(2023110518250000L))
    )

    val filteredPaths: List[Path] = FilterPathsBasedOnPredicate.filter(spark, List(basePath), predicates)

    val dataWithBasePath = ORCReader.read(spark, basePath.toString)
      .filter(col("string_partn_col").isin("val1", "val2")
        && col("double_partn_col") > 123.0
        && col("double_partn_col") < 235.0
        && col("float_partn_col").between(1.0F, 4.0F)
        && col("int_partn_col") === 234
        && col("long_partn_col") >= 2023110518230000L && col("long_partn_col") <= 2023110518250000L)

    val dataWithFilteredPaths = ORCReader.read(spark,
      Map("basePath" -> basePath.toString),
      filteredPaths.map(_.toString): _*)

    assert(dataWithBasePath.count() == dataWithFilteredPaths.count())
    assert(dataWithBasePath.columns.sorted sameElements dataWithFilteredPaths.columns.sorted)
    assert(filteredPaths.length == 48)
  }

  it should "return list of paths is some partition columns are skipped" in {
    val predicates: Map[Column, List[Predicate]] = Map(
      Column("double_partn_col") -> List(>=(123.0), <=(235.0)),
      Column("float_partn_col") -> List(between(1.0F, 4.0F)),
      Column("long_partn_col") -> List(>(2023110518230000L), <(2023110518250000L))
    )

    val filteredPaths: List[Path] = FilterPathsBasedOnPredicate.filter(spark, List(basePath), predicates)

    val dataWithBasePath = ORCReader.read(spark, basePath.toString)
      .filter(col("double_partn_col") >= 123.0
        && col("double_partn_col") <= 235.0
        && col("float_partn_col").between(1.0F, 4.0F)
        && col("long_partn_col") > 2023110518230000L && col("long_partn_col") < 2023110518250000L)

    val dataWithFilteredPaths = ORCReader.read(spark,
      Map("basePath" -> basePath.toString),
      filteredPaths.map(_.toString): _*)

    assert(dataWithBasePath.count() == dataWithFilteredPaths.count())
    assert(dataWithBasePath.columns.sorted sameElements dataWithFilteredPaths.columns.sorted)
    assert(filteredPaths.length == 54)
  }

  it should "throw an error if some column was not present as partition column" in {
    val predicates: Map[Column, List[Predicate]] = Map(
      Column("random_partn_col") -> List(>=(123.0), <=(235.0)),
      Column("float_partn_col") -> List(between(1.0F, 4.0F)),
      Column("long_partn_col") -> List(>(2023110518230000L), <(2023110518250000L))
    )

    assertThrows[PartitionColumnNotPresent](FilterPathsBasedOnPredicate.filter(spark, List(basePath), predicates))
  }

  it should "throw error" in {
    val predicates: Map[Column, List[Predicate]] = Map(
      Column("string_partn_col") -> List(equal("val1")),
      Column("double_partn_col") -> List(notEqual(123.5), in(234.6, 345.7)),
      Column("float_partn_col") -> List(in(1.0F, 2.1F, 3.2F)),
      Column("int_partn_col") -> List(in(234)),
      Column("long_partn_col") -> List(in(2023110518240000L, 2023110718240000L))
    )

    assertThrows[FileNotFoundException](
      FilterPathsBasedOnPredicate.filter(spark, List(new Path("basePath")), predicates))
  }

  it should "return list of paths with all in predicates" in {
    val predicates: Map[Column, List[Predicate]] = Map(
      Column("string_partn_col") -> List(equal("val1")),
      Column("double_partn_col") -> List(notEqual(123.5), in(234.6, 345.7)),
      Column("float_partn_col") -> List(in(1.0F, 2.1F, 3.2F)),
      Column("int_partn_col") -> List(in(234)),
      Column("long_partn_col") -> List(in(2023110518240000L, 2023110718240000L))
    )

    val filteredPaths: List[Path] = FilterPathsBasedOnPredicate.filter(spark, List(basePath), predicates)

    val dataWithBasePath = ORCReader.read(spark, basePath.toString)
      .filter(col("string_partn_col") === "val1"
        && col("double_partn_col") =!= 123.5
        && col("double_partn_col").isin(234.6, 345.7)
        && col("float_partn_col").isin(1.0, 2.1, 3.2)
        && col("int_partn_col").isin(234)
        && col("long_partn_col").isin(2023110518240000L, 2023110718240000L))

    val dataWithFilteredPaths = ORCReader.read(spark,
      Map("basePath" -> testingDirectory.path),
      filteredPaths.map(_.toString): _*)

    assert(dataWithBasePath.count() == dataWithFilteredPaths.count())
    assert(dataWithBasePath.columns.sorted sameElements dataWithFilteredPaths.columns.sorted)
    assert(filteredPaths.length == 24)
  }

  it should "return list of paths with < and <=, >, >= and == predicates" in {
    val predicates: Map[Column, List[Predicate]] = Map(
      Column("string_partn_col") -> List(equal("val1")),
      Column("double_partn_col") -> List(notEqual(123.5), in(234.6, 345.7)),
      Column("float_partn_col") -> List(<(3.4F), >(0.9F), equal(3.2F)),
      Column("int_partn_col") -> List(<(235), >(121) ,<=(234), >=(122)),
      Column("long_partn_col") -> List(equal(2023110518240000L))
    )

    val filteredPaths: List[Path] = FilterPathsBasedOnPredicate.filter(spark, List(basePath), predicates)

    assert(filteredPaths.length == 8)
  }
}
