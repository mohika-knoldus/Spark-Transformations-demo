package global.nashtech

import org.apache.spark.sql.SparkSession

object WordCount extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Count Number Each Words")
    .getOrCreate()

  //reading a text file into RDD
  val textRdd = spark.sparkContext.textFile("/home/knoldus/Desktop/DataTransformationsDemo/src/main/resources/book.txt")

  //splitting different words
  val mappedRdd = textRdd.flatMap(lines => lines.split(" "))

  //pairing each word with initial count 1
  val pairedRdd = mappedRdd.map(word => (word, 1))

  //adding values for similar words
  val reducedRdd = pairedRdd.reduceByKey(_ + _)

  //show the result
  reducedRdd.foreach(println)


}
