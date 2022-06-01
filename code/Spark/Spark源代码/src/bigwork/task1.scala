package bigwork

import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
import org.ansj.domain.Result
import org.ansj.domain.Term
import org.ansj.library.DicLibrary
import org.ansj.recognition.impl
import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.DicAnalysis

object task1 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("NameSegmenter").setMaster("local")
    val sc=new SparkContext(conf)
    val lines=sc.textFile("/home/wangming/Desktop/data")
    val dic_lines=Source.fromFile("/home/wangming/Desktop/people_name_list.txt")
    for(dic_line<-dic_lines.getLines())
      {
        DicLibrary.insert(DicLibrary.DEFAULT,dic_line)
      }
    val stop=new StopRecognition()
    stop.insertStopNatures("n","t","s","f","v","s","f","v","a","b","z","r","m","q","d","p","c","u","e","y","o","h","x","w","nr","nr1","nr2","nrj","nrf","ns","nsf","nt","nz","nl","ng","nw")
    stop.insertStopNatures("tg","vd","vn","vshi","vyou","vf","vx","vi","vl","vg")
    stop.insertStopNatures("ad","an","ag","al","bl","mq","qv","qt","pba","pbei","cc")
    stop.insertStopNatures("rr","rz","rzt","zs","rzv","ry","ryt","rys","ryv","rg")
    stop.insertStopNatures("uzhe","ule","uguo","ude1","ude2","ude3","usuo","udeng","uyy","udh","uls","uzhi","ulian")
    stop.insertStopNatures("k","xx","xu","wkz","wky","wyz","wyy","wj","ww","wt","wd","wf","wn","wm","ws","wp","wb","wh")
    stop.insertStopNatures("null")
    stop.insertStopNatures("i","l","j","dg","x","yg","g")
    stop.insertStopRegexes("^[a-zA-Z]{1,}")
    stop.insertStopRegexes("^[0-9]+")
    val result=lines.map{line => DicAnalysis.parse(line).recognition(stop).toStringWithOutNature(" ")}.filter(word=> !word.equals(""))
    result.saveAsTextFile("/home/wangming/Desktop/task1out_spark")
  }
}