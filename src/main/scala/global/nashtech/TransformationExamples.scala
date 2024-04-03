package global.nashtech

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object TransformationExamples extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Union Example")
    .getOrCreate()

  val projectAEmployee = Seq(("James", "Sales", "NY", 90000, 34, 10000),
    ("Michael", "Sales", "CA", 86000, 56, 20000),
    ("Robert", "Sales", "CA", 81000, 30, 23000),
    ("Maria", "Finance", "CA", 90000, 24, 23000)
  )

  import spark.implicits._

  val projectAEmployeeDf = projectAEmployee.toDF("employee_name", "department", "state", "salary", "age", "bonus")

  val projectBEmployee = Seq(("James", "Sales", "NY", 90000, 34, 10000),
    ("Maria", "Finance", "CA", 90000, 24, 23000),
    ("Jen", "Finance", "NY", 79000, 53, 15000),
    ("Jeff", "Marketing", "CA", 80000, 25, 18000),
    ("Kumar", "Marketing", "NY", 91000, 50, 21000)
  )

  val projectBEmployeeDf = projectBEmployee.toDF("employee_name", "department", "state", "salary", "age", "bonus")

  //Union returns all records irrespective of duplicate rows
  val allEmployees = projectAEmployeeDf.union(projectBEmployeeDf)

  allEmployees.show(false)

  //to eliminate the duplicate rows we can use distinct
  val employeeDf = allEmployees.distinct()
  employeeDf.show(false)

  //get the common records
  projectAEmployeeDf.intersect(projectBEmployeeDf).show(false)

}
