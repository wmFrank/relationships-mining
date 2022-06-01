package task5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class RelationshipGraphBuildReducer extends Reducer<Text, Text, Text, Text>  {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        LinkedHashMap<String,String> map = new LinkedHashMap<String, String>();
        Iterator<Text> it1 = values.iterator();
        while (it1.hasNext())
        {
            String curNameNum = it1.next().toString();
            String curName = curNameNum.split(",")[0];
            String curNum = curNameNum.split(",")[1];
            map.put(curName,curNum);
        }
        StringBuilder toWriter = new StringBuilder();
        Iterator iterator = map.entrySet().iterator();
        while(iterator.hasNext()) {
            Map.Entry<String,String> me = (Map.Entry<String,String>)iterator.next();
            toWriter.append(me.getKey() + "," + me.getValue() + ";");
        }
        toWriter.deleteCharAt(toWriter.length()-1);
        context.write( key ,new Text(key.toString() + ":" + toWriter.toString()));
    }

}
