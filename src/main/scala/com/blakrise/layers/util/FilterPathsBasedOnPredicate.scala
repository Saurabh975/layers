package com.blakrise.layers.util

import com.blakrise.layers.common.Predicate._
import com.blakrise.layers.common.{ColVal, Column, Predicate}
import com.blakrise.layers.exceptions.PartitionColumnNotPresent
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.annotation.tailrec

object FilterPathsBasedOnPredicate {

  /**
   * Takes List of Paths and return List of Paths  after applying the predicates on it.
   *
   * NOTE : THIS METHOD IS EXCLUSIVE TO FILTER PATHS BASED ON PARTITION COLUMNS.
   *
   * It throws an error if the column you provided is not a Partition Column.
   *
   * To know more about partition column,
   * refer here -> https://sparkbyexamples.com/spark/spark-partitioning-understanding/
   *
   * @param spark      Spark Session
   * @param paths      List of paths on which the predicates have to be applied
   * @param predicates Map of column name and list of predicates to be applied on that column
   * @param splitOn    if your paths looks like -> basePath/partnCol1=partnVal1/partnCol2=partnVal2
   *                   then the splitOn value is "=". Default value is "="
   * @return List of paths after applying predicates to it
   * @throws PartitionColumnNotPresent with list of Column which were not found
   */
  def filter(spark: SparkSession, paths: List[Path], predicates: Map[Column, List[Predicate]],
             splitOn: String = "="): List[Path] = {
    filterWithLeafIndicator(spark, paths, predicates, splitOn)
  }

  /**
   * Takes List of Paths and return List of Paths  after applying the predicates on it.
   *
   * NOTE : THIS METHOD IS EXCLUSIVE TO FILTER PATHS BASED ON PARTITION COLUMNS.
   *
   * It throws an error if the column you provided is not a Partition Column.
   *
   * @param spark         SparkSession
   * @param paths         List of Paths on which the predicates ahve to applied
   * @param predicates    Map of column name and list of predicates to be applied on that column
   * @param splitOn       if your paths looks like -> basePath/partnCol1=partnVal1/partnCol2=partnVal2
   *                      then the splitOn value is "=". Default value is "="
   * @param isLeafReached This keeps a track if we have reached the Leaf files in the process of
   *                      filtering out partition columns
   * @return List of paths after applying predicates to it
   * @throws PartitionColumnNotPresent with list of Column which were not found
   */
  @tailrec private def filterWithLeafIndicator(spark: SparkSession, paths: List[Path],
                                               predicates: Map[Column, List[Predicate]], splitOn: String = "=",
                                               isLeafReached: Boolean = false): List[Path] = {

    // No Predicates left to filter paths on
    if (predicates.isEmpty) return paths

    //All paths filtered out or leaf reached and yet Predicates are left
    if ((paths.isEmpty || isLeafReached) && predicates.nonEmpty) {
      val exceptionMessage = predicates.keySet.mkString(", ")
      throw PartitionColumnNotPresent(s"Partition Columns not found - $exceptionMessage")
    }

    //All paths filtered out and no predicates left
    if (paths.isEmpty) return paths

    val partnColName: Column = extractLastColName(paths.head, splitOn)

    predicates.get(partnColName) match {
      case Some(listOfPredicates: List[Predicate]) =>
        val filteredPaths = filterPathsBasedOnPredicate(createMapOfColValueAndPath(paths), listOfPredicates)
        val (pathWithImmediateChildDirectory, isLeaf) = getImmediateChildDirectory(spark, filteredPaths)
        filterWithLeafIndicator(spark, pathWithImmediateChildDirectory, predicates - partnColName, splitOn, isLeaf)

      case None =>
        val (pathWithImmediateChildDirectory, isLeaf) = getImmediateChildDirectory(spark, paths)
        filterWithLeafIndicator(spark, pathWithImmediateChildDirectory, predicates, splitOn, isLeaf)
    }
  }

  /**
   * Applies Predicates to the List of paths
   *
   * @param paths      map of list of Paths grouped together on the basis of Partition Column Value
   * @param predicates List of predicates
   * @return List of paths after applying the predicates to it
   */
  @tailrec private def filterPathsBasedOnPredicate(paths: Map[ColVal, List[Path]], predicates: List[Predicate])
  : List[Path] = {
    if (paths.isEmpty || predicates.isEmpty) {
      convertMapToListOfPath(paths)
    } else {
      val operator = predicates.head
      lazy val tail = predicates.tail

      operator match {
        case <(_) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
        case <=(_) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
        case >(_) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
        case >=(_) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
        case between(_, _) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
        case equal(_) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
        case notEqual(_) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
        case in(_*) => filterPathsBasedOnPredicate(paths.filter(path => operator(path._1)), tail)
      }
    }
  }

  /**
   * Converts Map[ColVal, List[Path] to List[Path]
   *
   * @param paths : Map of ColVal, List[Path]
   * @return List[Path]
   */
  private def convertMapToListOfPath(paths: Map[ColVal, List[Path]]): List[Path] = paths
    .foldLeft(List.empty[Path])((acc, pathList) => acc ::: pathList._2)

  /**
   * This method return Array of paths till immediate child directory of all the paths provided as input
   *
   * case 1: the input paths has child directory -> paths with immediate child directory is returned
   *
   * eg: path = basePath/partnCol1=partnval1/partnCol3=partnval2/partnCol3=partnval3
   *
   * return - basePath/partnCol1=partnval1/partnCol3=partnval2/partnCol3=partnval3/partnCol4=partnVal4
   *
   * case 2: the input paths has only leaf files in it -> paths with leaf files are returned
   *
   * eg: path = basePath/partnCol1=partnval1/partnCol3=partnval2/partnCol3=partnval3/partnCol4=partnVal4
   *
   * return - basePath/partnCol1=partnval1/partnCol3=partnval2/partnCol3=partnval3/partnCol4=partnVal4/file.orc
   *
   * @param spark Spark Session
   * @param paths List of path whose immediate child directory is required
   * @return List of path and a flag if leaf file was reached
   */
  private def getImmediateChildDirectory(spark: SparkSession, paths: List[Path]): (List[Path], Boolean) = {
    val hc: Configuration = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(hc)
    val listStatus: List[FileStatus] = paths.flatMap(fs.listStatus(_))
    val immediateChildDirectory: List[Path] = listStatus.filter(_.isDirectory).map(_.getPath)
    immediateChildDirectory.length match {
      case 0 =>
        val isLeaf = true
        (listStatus.filter(_.isFile).map(_.getPath), isLeaf)
      case _ =>
        val isLeaf = false
        (immediateChildDirectory, isLeaf)
    }
  }

  /**
   * This method groups List of Path on the basis of partnColVal
   *
   * @param paths   List of path
   * @param splitOn if your paths looks like -> basePath/partnCol1=partnVal1/partnCol2=partnVal2
   *                then the splitOn value is "=". Default value is "="
   * @return Map of ColVal and List[Path]
   */
  private def createMapOfColValueAndPath(paths: List[Path], splitOn: String = "="): Map[ColVal, List[Path]] = paths
    .foldLeft(Map.empty[ColVal, List[Path]]) { (map, path) =>
      val colValue = extractLastColValue(path, splitOn)
      map.updated(colValue, map.get(colValue).fold(List(path))(_ :+ path))
    }

  /**
   * Extracts the col name out of the path
   *
   * eg - path = basePath/partnCol1=partnval1/partnCol3=partnval2/partnCol3=partnval3
   *
   * return partnCol3
   *
   * @param path Path
   * @param splitOn default val =
   * @return The Last Partition Column Name
   * */
  private def extractLastColName(path: Path, splitOn: String = "="): Column = Column(path.getName.split(splitOn).head)

  /**
   * Extracts the last partition value out of the path
   *
   * eg p ath = basePath/partnCol1=partnval1/partnCol3=partnval2/partnCol3=partnval3
   *
   * return partnval3
   *
   * @param path path
   * @param splitOn default val =
   * @return ColVal
   */
  private def extractLastColValue(path: Path, splitOn: String = "="): ColVal = ColVal(path.getName.split(splitOn).last)
}

