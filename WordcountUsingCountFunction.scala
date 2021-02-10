
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._

object WordcountUsingCountFunction {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
val conf=new SparkConf().setMaster("local")
  .setAppName("WordLength")


val sc = new SparkContext(conf)
    val data=sc.textFile("E:/SparkScala/text.txt")
    val mapFile=data.flatMap(line =>line.split(" ")).filter(value => value=="Bangalore")
    val wordlength = mapFile.map(word => (word, 1)).reduceByKey((x:Int,y:Int)=>x+y)


    wordlength.foreach(println)

println("")

    println (mapFile.countByValue())      /// using count by value




  }

}
