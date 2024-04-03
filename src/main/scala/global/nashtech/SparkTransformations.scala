package global.nashtech

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SparkTransformations extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Big Data Transformations Using Spark")
    .getOrCreate()

  val transactionRawDf = spark.read.options(Map(("inferSchema", "true"), ("header", "true")))
    .csv("/home/knoldus/Desktop/DataTransformationsDemo/src/main/resources/Sales Transaction v.4a.csv")

  //get the number of rows in dataframe : 536350
  println(transactionRawDf.count())

  /*
  @TransactionNo -a six-digit unique number that defines each transaction
  @Date - the date when each transaction was generated
  @ProductNo - a five or six-digit unique character used to identify a specific product.
  @ProductName - product/item name.
  @Price - the price of each product per unit in pound sterling
  @Quantity - the quantity of each product per transaction. Negative values related to cancelled transactions.
  @CustomerNo - a five-digit unique number that defines each customer.
  @Country - name of the country where the customer resides.
   */
  //what does this dataset contains
  transactionRawDf.show()

  //filter all the negative transactions
  val validTransactionDf = transactionRawDf.filter(col("Quantity") > 0)
  validTransactionDf.show()


  //*- Calculating net sales per month for year 2019 -*

  //transforming date column to desired format
  //filtering records for a year
  //extracting sales for month by grouping the data by month and summing up sales
  val salesDf = validTransactionDf.withColumn("Date", to_date(col("Date"), "M/d/yyyy"))
    .filter(year(col("Date")) === 2019)
    .withColumn("month", month(col("Date"))).groupBy("month")
    .agg(round(sum(col("Price") * col("Quantity")), 2).as("MonthlySales")).orderBy(col("month"))

  salesDf.show()

  //Get to know the month with maximum sales
  val maximumSale = salesDf.agg(max(col("MonthlySales"))).first().getDouble(0)
  println(maximumSale)

  val maximumSaleMonth = salesDf.select("*").where(col("MonthlySales") === maximumSale)
  maximumSaleMonth.show()

  //Product most sold
  val popularProductDf = validTransactionDf.groupBy(col("ProductNo"), col("ProductName")).agg(sum(col("Quantity"))
      .as("TotalProductsSold")).orderBy(col("TotalProductsSold").desc)
    .select("*").limit(5)

  popularProductDf.show(false)

  //popular product country wise
  val countryGroupedDf = validTransactionDf.groupBy(col("Country"), col("ProductNo"),
    col("ProductName")).agg(sum(col("Quantity")).as("TotalQuantity"))

  countryGroupedDf.show()
  //windowing
  val windowSpec = Window.partitionBy(col("Country")).orderBy(col("TotalQuantity").desc)

  val popularProductPerCountry = countryGroupedDf.withColumn("rank", rank().over(windowSpec))
    .filter(col("rank") === 1)

  popularProductPerCountry.show()
}
