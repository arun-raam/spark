import org.apache.spark.{SparkConf, SparkContext}

import java.util.logging.{Level, Logger}


object RddActionsandFunctions {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("collect").setMaster("local")
    val sc = new SparkContext(conf)

    val inputWords1 = List("spark", "hadoop", "spark", "hive", "pig","Mlib","spark sql", "cassandra", "hadoop")
    val inputWords2 = List("spark","spark core","spark sql","hadoop","spark streaming","Mlib","Mlib", "GraphX","GraphX")

    val wordRdd1 = sc.parallelize(inputWords1)
    val wordRdd2 = sc.parallelize(inputWords2)

    val RddUnion = wordRdd1.union(wordRdd2)                  // using Union

    val RddIntersection=wordRdd1.intersection(wordRdd2)       // using intersection

    val RddDistinct1= wordRdd1.distinct()                             //using distinct
    val RddDistinct2=wordRdd2.distinct()



    val words1 = RddUnion.collect()                     // using collect
    val words2 =RddIntersection.collect()

    words1.foreach(println)                         // printing result for union
    println("")
    words2.foreach(println)                         //printing result for intersection
    println("")
    RddDistinct1.foreach( println)
    println("")
    RddDistinct2.foreach( println)                   //printing for distinct

   val res =RddUnion.top(3)                                //getting top 3 elements
  res.foreach(println)

  }







}
