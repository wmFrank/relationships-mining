package task5;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class LabelPropagationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        LinkedHashMap<String,Integer> hash = new LinkedHashMap<String, Integer>();
        String ls = "";
        String result = null;
        for(Text value:values) {
            String tmp = value.toString();
            if(tmp.startsWith("$")){
                ls = ":" + tmp.substring(tmp.indexOf("$") + 1);
            }
            else {
                String[] segs = tmp.split(",");
                String label = segs[0];
                int num = Integer.parseInt(segs[1]);
                if(hash.containsKey(label)){
                    hash.put(label,hash.get(label) + num);
                }
                else{
                    hash.put(label,num);
                }
                Iterator iterator = hash.entrySet().iterator();
                int max = 0;
                while(iterator.hasNext()) {
                    Map.Entry<String,Integer> me = (Map.Entry<String,Integer>)iterator.next();
                    if(me.getValue() > max){
                        max = me.getValue();
                        result = me.getKey();
                    }
                }
            }
        }
        context.write(key, new Text(result + ls));
    }
}
