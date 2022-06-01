package bigwork

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.HashSet
import scala.Iterable

import java.util.Iterator
import java.util.LinkedHashMap

object task5 {
  def main(args: Array[String]) {
    def conf = new SparkConf().setAppName("LabelPropagationer").setMaster("local")
    def sc = new SparkContext(conf)
    val data = sc.textFile("/home/wangming/Desktop/task2out_spark")
    type mytype=(String,Double)
    val mapper=data.map{line=>(line.split(",")(0),line.split(",")(1))}.reduceByKey((x,y)=>x+";"+y)
    var result=mapper.map({line=>
      val value=line._2.replaceAll("\\t",",")
      val result=line._1+"\t"+ line._1 + ":" + value
      result
    })
    val times = 30
    for(i <- 0 until times){
      result = result.flatMap(line => labelpropagationmapper(line)).filter(line => !line.equals(("",""))).groupByKey().map(line => labelpropagationreducer(line))
    }
    result.saveAsTextFile("/home/wangming/Desktop/task5out_spark")
    sc.stop()
  }
  
  def labelpropagationmapper(line : String) : HashSet[(String,String)] = {
    val result = HashSet[(String,String)]()
    val ret = line.split("\t")
    val person = ret.apply(0)
    val segs = ret.apply(1).split(":")
    val label = segs.apply(0)
    if(segs.length > 1){
      val page_list = segs.apply(1).split(";")
      for(i <- 0 until page_list.length){
        val parts = page_list.apply(i).split(",")
        val man = parts.apply(0)
        val num = parts.apply(1)
        result.add((man,label + "," + num))
      }
      result.add((person,"$" + segs.apply(1)))
    }
    return result
  }
  
  def labelpropagationreducer(line : (String,Iterable[String])) : String = {
    val hash = new LinkedHashMap[String,Int]()
    var ls : String = null
    var ret : String = null
    val d = 0.85
    val it = line._2.iterator
    while(it.hasNext){
      val tmp = it.next()
      if(tmp.startsWith("$")){
        ls = ":" + tmp.substring(tmp.indexOf("$") + 1)
      }
      else{
        val segs = tmp.split(",")
        val label = segs.apply(0)
        val num = segs.apply(1).toInt
        if(hash.containsKey(label)){
          hash.put(label, hash.get(label) + num)
        }
        else{
          hash.put(label,num)
        }
        val iterator = hash.entrySet().iterator()
        var max = 0
        while(iterator.hasNext()){
          val me = iterator.next()
          if(me.getValue > max){
            max = me.getValue
            ret = me.getKey
          }
        }
      }
    }
    val result = line._1 + "\t" + ret + ls
    return result
  }
}