package global.nashtech

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkJoins extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Understanding Joins In Spark")
    .getOrCreate()

  import spark.implicits._

  val employeeData = Seq((1, "John", 101),
    (2, "Alice", 102), (3, "Jenn", 103), (4, "Daniel", 104))

  val employeeDF = employeeData.toDF("SNo", "Name", "DeptId")
  employeeDF.show(false)

  val departmentData = Seq((101, "HR"), (102, "Marketing"), (103, "Engineering"), (105, "Finance"))
  val departmentDF = departmentData.toDF("DeptId", "DeptName")
  departmentDF.show(false)

  //inner join
  //default join, if not specified , inner join will be performed
  println("INNER JOIN")
  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "inner")
    .show(false)

  //outer join or full outer join , fills with null where records doesn't exist
  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "outer")
    .show(false)

  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "full")
    .show(false)

  //left join : get records where matched and records from left table

  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "left")
    .show(false)

  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "leftouter")
    .show(false)

  //right rightouter : get records where matched and all from right table

  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "right")
    .show(false)

  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "rightouter")
    .show(false)

  //left-semi : get rows from left dataframe that match with some rows from right dataset
  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "leftsemi")
    .show(false)

  //left-anti : get the row that is not found in the right dataframe
  employeeDF.join(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "leftanti")
    .show(false)

  //self-join
  employeeDF.as("emp1")
    .join(employeeDF.as("emp2"),
      col("emp1.DeptId") === col("emp2.DeptId"), "inner")
    .select(col("emp1.SNo"), col("emp1.DeptId"), col("emp1.Name"), col("emp2.Name"))
    .show(false)

  //using joinWith to return tuples
  employeeDF.joinWith(departmentDF, employeeDF("DeptId") === departmentDF("DeptId"), "inner").show(false)


}

