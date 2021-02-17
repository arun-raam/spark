import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SparkSqlDataframeFunctions {
  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    Logger.getLogger("org").setLevel(Level.ERROR)

    val df = spark.read.json("E:/SparkScala/people.json")

    df.show()                //print function
    df.printSchema()             // print schema
    df.select("id").show()           // using select function
    df.select($"name",$"age"+1).show()       // using dollar sign
    df.filter($"age">18).show()            //checking condition
    df.groupBy($"age").count().show()                //using count function
    df.createOrReplaceTempView("arun")                // creating temp view
   val sqlQuery= spark.sql("SELECT * FROM arun").show()                 //  way of using sql query

  }



}

