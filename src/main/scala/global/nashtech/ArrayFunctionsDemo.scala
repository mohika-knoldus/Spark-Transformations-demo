package global.nashtech

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ArrayFunctionsDemo extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Understanding Array Functions")
    .getOrCreate()

  // Sample DataFrame
  val data = Seq(
    (1, Array(44, 34, 23), Array(44, 32, 43)),
    (2, Array(43, 34, 24), Array(24, 45, 34)),
    (3, Array(37, 45, 30), Array(30, 35, 37))
  )

  val columns = Seq("ID", "MarksSem1", "MarksSem2")

  val df = spark.createDataFrame(data).toDF(columns: _*)
  df.show(false)

  // array_contains
  println("array_contains")
  df.withColumn("arrayContainsResult", array_contains(col("MarksSem1"), 34)).show()

  // array_distinct
  //Removes duplicate values from the array.
  println("array_distinct")
  df.withColumn("distinct_value", array_distinct(col("MarksSem1"))).show()

  // array_except
  //except : returns elements from first array not in second array
  println("array_except")
  df.withColumn("except_values", array_except(col("MarksSem1"), col("MarksSem2"))).show()

  // array_intersect
  //This function returns common elements from both arrays. This is logically equivalent to set intersection operation.
  println("array_intersect")
  df.withColumn("IntersectValues", array_intersect(col("MarksSem1"), col("MarksSem2"))).show()

  // array_join
  //This Function joins all the array elements based on delimiter defined as the second argument.
  println("array_join")
  df.withColumn("JoinedValues", array_join(col("MarksSem1"), ",")).show()

  // array_max
  //This function returns the maximum value from an array.
  println("array_max")
  df.withColumn("MaxValue", array_max(col("MarksSem1"))).show()

  // array_min
  //This function returns the minimum value from an array.
  println("array_min")
  df.withColumn("MinValue", array_min(col("MarksSem1"))).show()

  // array_position
  //This function returns the position of first occurrence of a specified element. If the element is not present it
  // returns 0.
  println("array_position")
  df.withColumn("PositionOf34", array_position(col("MarksSem1"), 34)).show()

  // array_remove
  // This function removes all the occurrences of an element from an array.
  println("array_remove")
  df.withColumn("ValuesWithout34", array_remove(col("MarksSem1"), 34)).show()

  // array_repeat
  //This function creates an array that is repeated as specified by second argument.
  println("array_repeat")
  df.withColumn("RepeatedValues", array_repeat(col("MarksSem1"), 2)).show(false)

  // array_sort
  //This function sorts the elements of an array in ascending order
  println("array_sort")
  df.withColumn("SortedValues", array_sort(col("MarksSem1"))).show()

  // array_union
  // This function returns the union of all elements from the input arrays.
  println("array_union")
  df.withColumn("UnionValues", array_union(col("MarksSem1"), col("MarksSem2"))).show()

  // array_overlap
  //    This function checks if at least one element is common/overlapping in arrays. It returns true if at least one
  //    element is common in both array and false otherwise. It returns null if at least one of the arrays is null.
  println("array_overlap")
  df.withColumn("is_overlap", arrays_overlap(col("MarksSem1"), col("MarksSem2"))).show()

  // arrays_zip
  // Returns a merged array of structs in which the N-th struct contains all N-th values of input arrays.
  println("arrays_zip")
  df.withColumn("ZippedValues", arrays_zip(col("MarksSem1"), col("MarksSem2"))).show()

  // concat
  //concatenates multiple input columns together into a single column. The function works with strings,
  // binary and compatible array columns.
  println("concat")
  df.withColumn("Concatenated", concat(col("ID").cast("string"), col("MarksSem1")(0)
    .cast("string"))).show()

  // element_at
  //Returns element of array at given index
  println("element_at")
  df.withColumn("ElementAt2", element_at(col("MarksSem1"), 2)).show()

  // exists
  //  The exists function is a higher-order function in Spark that checks whether a condition holds for at least one
  //  element in an array. In this case, it checks whether there exists an element in the "MarksSem1" array (column) where
  //  the value is equal to 34.
  println("exists")
  df.filter(exists(col("MarksSem1"), value => value === 34)).show()

  // Stop the Spark session
  spark.stop()
}
