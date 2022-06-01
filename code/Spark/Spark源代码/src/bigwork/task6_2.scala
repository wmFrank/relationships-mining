package bigwork

import org.apache.spark.{SparkContext, SparkConf}

object task6_2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("task1").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("/home/wangming/Desktop/task5out_spark")
    val res=lines.map({line=>
      val key=line.split(":")(0).split("\\t")(1)
      val value=line.split(":")(0).split("\\t")(0)
      (key,value)
    }).reduceByKey((x,y)=>(x+","+y))
    val result=res.map(line=>format(line))
    result.saveAsTextFile("/home/wangming/Desktop/task6_2out_spark")
  }
  def format(line : (String,String)) : String={
    var result = line._1 + "\t" + line._2
    return result
  }
}