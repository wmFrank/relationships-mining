package bigwork

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable._

object task2 {
  def main(args: Array[String]) {
    def conf = new SparkConf().setAppName("PeopleCooccur").setMaster("local")
    def sc = new SparkContext(conf)
    val data = sc.textFile("/home/wangming/Desktop/task1out_spark")
    val result = data.flatMap(line => mapper(line)).reduceByKey(_+_).map(line => format(line))
    result.repartition(1).saveAsTextFile("/home/wangming/Desktop/task2out_spark")
    sc.stop()
  }
  
  def mapper(line : String) : HashSet[(String,Int)] = {
    val result = HashSet[(String,Int)]()
    val rawStrings = line.split(" ");
    val str = HashSet[String]()
    for(i <- 0 until rawStrings.length){
      str.add(rawStrings.apply(i))
    }
    val it = str.iterator
    while(it.hasNext){
      val str1 = it.next()
      val it2 = str.iterator
      while(it2.hasNext){
        val str2 = it2.next()
        if(!str1.equals(str2)){
          val s = str1 + "," + str2
          result.add((s,1))
        }
      }
    }
    return result
  }
  
  def format(line : (String,Int)) : String = {
    val result = line._1 + "\t" + line._2.toString()
    return result
  }
}