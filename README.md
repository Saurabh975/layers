# LAYERS

### What Problem is it solving?
Well, when you read a DF using spark and then provide some filter conditions on it, 
Spark first lists down all the files that are present in the base path and then 
starts applying filter on it

Now, we are dealing with big data that means the data can be present in huge quantity.
So, why not give spark, only the relevant path, in which we are certain that data will be there.
This way, Apache Spark can leverage its distributed computing capabilities to 
efficiently process large volumes of data in parallel, and not focusing on how to optimize the read time


### So, here's Layers
- A simple util built to help optimize the spark read.
- This library is helpful when it comes to applying filter clauses on partition columns.

Let's take for example you have a dataframe on which you're applying filters, 


```
     val dfAfterApplyingFilters = df
       .filter(col("partn_col1") === "val1"
        && col("partn_col2") =!= 123.5
        && col("non_partn_col").isin(234.6, 345.7))
```

Then this can be optimized this way.
```
    val basePath: String = "base_path" // path from where you wanted to read values
    val predicates: Map[Column, List[Predicate]] = Map(
      Column("partn_col1") -> List(equal("val1")),
      Column("partn_col2") -> List(notEqual(123.5)))
      
    val filteredPaths: List[Path] = FilterPathsBasedOnPredicate.filter(spark, List(basePath), predicates)
    
    val dataWithFilteredPaths = ORCReader.read(spark,
      Map("basePath" -> basePath),
      filteredPaths.map(_.toString): _*)
      
    val dfAfterApplyingFilters = dataWithFilteredPaths
        .filter(col("non_partn_col").isin(234.6, 345.7))
```

More examples can be found in [How to use FilterPathsBasedOnPredicate](src/test/scala/io/github/saurabh975/layers/util/FilterPathsBasedOnPredicateTest.scala)

### Operators supported
- For Numeric values(Int, Long, Float Double only)
  - `<` 
  - `<=`
  - `>`
  - `>=`
  - `between`
  - `equal`
  - `notEqual`
  - `in`
  

- For String values
    - `equal`
    - `notEqual`
    - `in`


Apart from the above util, there's a [read util](src/main/scala/io/github/saurabh975/layers/reader/Reader.scala) too.
As of now, it supports 
- ORC Reader
- ParquetReader
- JSONReader
- CSVReader

### For build.sbt
``` 
"io.github.saurabh975" %% "layers" % "1.0.0" 
```