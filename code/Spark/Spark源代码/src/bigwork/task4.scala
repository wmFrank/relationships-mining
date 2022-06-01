package bigwork

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.HashSet
import scala.Iterable

object task4 {
  def main(args: Array[String]) {
    def conf = new SparkConf().setAppName("PageRanker").setMaster("local")
    def sc = new SparkContext(conf)
    val data = sc.textFile("/home/wangming/Desktop/task3out_spark")
    var result = data.map(line => graphbuildmapper(line)).map(line => format1(line))
    val times = 30
    for(i <- 0 until times){
      result = result.flatMap(line => pagerankmapper(line)).filter(line => !line.equals(("",""))).groupByKey().map(line => pagerankreducer(line))
    }
    result.saveAsTextFile("/home/wangming/Desktop/task4out_spark")
    sc.stop()
  }
  
  def graphbuildmapper(line : String) : (String,String) = {
    val pagerank = "1.0:"
    val ret = line.split("\t")
    val person = ret.apply(0)
    val person_list = ret.apply(1)
    val pr = pagerank + person_list
    return (person,pr)
  }
  
  def pagerankmapper(line : String) : HashSet[(String,String)] = {
    val result = HashSet[(String,String)]()
    val ret = line.split("\t")
    val person = ret.apply(0)
    val segs = ret.apply(1).split(":")
    val pr = segs.apply(0).toDouble
    if(segs.length > 1){
      val page_list = segs.apply(1).split(";")
      for(i <- 0 until page_list.length){
        val parts = page_list.apply(i).split(",")
        val man = parts.apply(0)
        val num = parts.apply(1)
        val prvalue = pr * num.toDouble
        result.add((man,prvalue.toString()))
      }
      result.add((person,"$" + segs.apply(1)))
    }
    return result
  }
  
  def pagerankreducer(line : (String,Iterable[String])) : String = {
    var ls : String = null
    var pr : Double = 0.0
    val d = 0.85
    val it = line._2.iterator
    while(it.hasNext){
      val tmp = it.next()
      if(tmp.startsWith("$")){
        ls = ":" + tmp.substring(tmp.indexOf("$") + 1)
      }
      else{
        pr += tmp.toDouble
      }
    }
    pr = 1 - d + d * pr
    val result = line._1 + "\t" + pr.toString() + ls
    return result
  }
  
  def format1(line : (String,String)) : String = {
    val result = line._1 + "\t" + line._2
    return result
  }
}