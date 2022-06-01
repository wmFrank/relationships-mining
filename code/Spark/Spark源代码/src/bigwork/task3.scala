package bigwork

import org.apache.spark.{SparkContext, SparkConf}

object task3 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("RelationshipGrapher").setMaster("local")
    val sc=new SparkContext(conf)
    val data=sc.textFile("/home/wangming/Desktop/task2out_spark")
    type mytype=(String,Double)
    var totalnum=0.0
    val mapper=data.map{line=>(line.split(",")(0),line.split(",")(1))}.reduceByKey((x,y)=>x+";"+y)
    val reducer=mapper.map({line=>
      val value=line._2.replaceAll("\\t",",").split(";")
      var value_2=value.map(line=>(line.split(",")(0),line.split(",")(1).toDouble))
      for(i<-0 until value_2.length) {
        totalnum=totalnum+value_2(i)._2
      }
      var summer:String=""
      for(i<-0 until value_2.length)
        {
          val name=value_2(i)._1
          val sum=value_2(i)._2/totalnum
          summer=summer+name+","+sum+";"
        }
      totalnum=0.0
      val result=line._1+"\t"+summer
      result
    })
    reducer.saveAsTextFile("/home/wangming/Desktop/task3out_spark")
  }
}