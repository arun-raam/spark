import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object SparkWordCount {
  def main(args: Array[String]) {
      //Create conf object
      val conf = new SparkConf().setMaster("local")
        .setAppName("WordCount")
      //create spark context object
      val sc = new SparkContext(conf)
      val rawData = sc.textFile("E:/SparkScala/test.txt")
      //convert the lines into words using flatMap operation
      val words = rawData.flatMap(line => line.split(" "))
      //count the individual words using map and reduceByKey operation
      val wordCount = words.map(word => (word, 1)).reduceByKey(_ + _)
      wordCount.foreach(println)
      //stop the spark context
      sc.stop
    }



}
