import org.apache.spark._
import org.apache.spark.streaming._



object SparkStreaming {

    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
      val ssc = new StreamingContext(conf, Seconds(5))
   //   val lines = ssc.socketTextStream("localhost", 9999)
//      val words = lines.flatMap(_.split(" ")) // not necessary since Spark 1.3
      // Count each word in each batch
//      val pairs = words.map(word => (word, 1))
 //     val wordCounts = pairs.reduceByKey(_ + _)

      // Print the first ten elements of each RDD generated in this DStream to the console
//      wordCounts.print()
//      ssc.start()             // Start the computation
//      ssc.awaitTermination()  // Wait for the computation to terminate

//      val words = ssc.socketTextStream("localhost", 9999)
//      val ans = words.map { word => ("hello" ,word ) } // map hello with each line
//      ans.print()
//      ssc.start() // Start the computation
//      ssc.awaitTermination() // Wait for termination
//


      val lines = ssc.socketTextStream("localhost", 9999)
      val words = lines.flatMap(_.split(" "))
      val output = words.filter { word => word.startsWith("s") } // filter the words starts with letter“s”
      output.print()
      output.countByValue().print()

      ssc.start()
      ssc.awaitTermination()





    }




}
