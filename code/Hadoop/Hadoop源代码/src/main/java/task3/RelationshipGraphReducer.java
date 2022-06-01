package task3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class RelationshipGraphReducer extends Reducer<Text, Text, Text, Text>  {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        LinkedHashMap<String,String> map = new LinkedHashMap<String, String>();
        Iterator<Text> it1 = values.iterator();
        double totalNum = 0.0;
        while (it1.hasNext())
        {
            String curNameNum = it1.next().toString();
            String curName = curNameNum.split(",")[0];
            String curNum = curNameNum.split(",")[1];
            map.put(curName,curNum);
            totalNum += Integer.parseInt(curNum);
        }
        StringBuilder toWriter = new StringBuilder();
        Iterator iterator = map.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<String,String> me = (Map.Entry<String,String>)iterator.next();
            double weight = Integer.parseInt(me.getValue()) / totalNum;

            toWriter.append(me.getKey() + "," + weight + ";");
        }
        toWriter.deleteCharAt(toWriter.length()-1);
        //Iterator<Text> it2 = values.iterator();
        //String toWriter = "[";
        //StringBuilder toWriter=new StringBuilder("[");
        /*while (it2.hasNext())
        {
            String curNameNum = it2.next().toString();
            String curName = curNameNum.split(",")[0];
            String curNum = curNameNum.split(",")[1];
            double weight = Integer.parseInt(curNum) / totalNum;
            throw new IOException("wocwoc!!!!!");
            //toWriter = toWriter.concat(curName + "," + String.format("%.2f",weight) + ";");
            //toWriter.append(new String(curName + "," + String.format("%.2f",weight) + "|"));
            //toWriter.append(it2.next().toString());
        }
        toWriter += "]";*/
        context.write( key ,new Text(toWriter.toString()));

    }

}
