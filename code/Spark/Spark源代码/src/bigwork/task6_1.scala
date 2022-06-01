package bigwork

import org.apache.spark.{SparkContext, SparkConf}

object task6_1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("task1").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("/home/wangming/Desktop/task4out_spark")
    val res=lines.map({ line =>
      val values=line.split(":")(0)
      val key=values.split("\\t")(1).toDouble
      val value=values.split("\\t")(0)
      (key,value)
    }).sortByKey(false).map(line => format(line))
    res.saveAsTextFile("/home/wangming/Desktop/task6_1out_spark")
  }
  def format(line : (Double,String)) : String={
    var result = line._1.toString() + "\t" + line._2
    return result
  }
}