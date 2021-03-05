import org.apache.spark.sql.SparkSession

object SparkSqlDataFrameUsingCsv {

  case class Department(id: String, name: String)
  case class Employee(firstName: String, lastName: String, email: String, salary: Int)
  case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])



  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().master("local").appName("data frame functions ").getOrCreate()




    // Create the Departments
    val department1 = new Department("123456", "Computer Science")
    val department2 = new Department("789012", "Mechanical Engineering")
    val department3 = new Department("345678", "Theater and Drama")
    val department4 = new Department("901234", "Indoor Recreation")



    // Create the Employees
    val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
    val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
    val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
    val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)
    val employee5 = new Employee("michael", "jackson", "no-reply@neverla.nd", 80000)

    // Create the DepartmentWithEmployees instances from Departments and Employees
    val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
    val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
    val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee5, employee4))
    val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))

    import spark.implicits._

    val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
    val dataframe1 = departmentsWithEmployeesSeq1.toDF()


    val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
    val dataframe2 = departmentsWithEmployeesSeq2.toDF()


//   val unionDF = dataframe1.union(dataframe2)
//
//   unionDF.show()
//
//
//    unionDF.write.parquet("E:/SparkScala/databricks-df-example.parquet")



    val parquetDF = spark.read.parquet("E:/SparkScala/databricks-df-example.parquet")
    parquetDF.show()



    import org.apache.spark.sql.functions._

    val explodeDF = parquetDF.select(explode($"employees"))
    explodeDF.show()

    val flattenDF = explodeDF.select($"col.*")
    flattenDF.show()

//
//    val filterDF = flattenDF
//      .filter($"firstName" === "xiangrui" || $"firstName" === "michael")
//      .sort($"lastName".asc)
//    filterDF.show()
//
//    val whereDF = flattenDF
//      .where($"firstName" === "xiangrui" || $"firstName" === "michael")
//      .sort($"lastName".desc)
//    whereDF.show()


    val nonNullDF = flattenDF.na.fill("--")
    nonNullDF.show()


    // Find the distinct last names for each first name
    val countDistinctDF = nonNullDF.select($"firstName", $"lastName")
      .groupBy($"firstName")
     .agg(countDistinct($"lastName") as "distinct_last_names")
    countDistinctDF.show()

    countDistinctDF.explain()




    // register the DataFrame as a temp view so that we can query it using SQL
    nonNullDF.createOrReplaceTempView("databricks_df_example")

    spark.sql("""
  SELECT firstName, count(distinct lastName) as distinct_last_names
  FROM databricks_df_example
  GROUP BY firstName
""").explain


    val salarySumDF = nonNullDF.agg("salary" -> "min")
    salarySumDF.show()
  }

}
