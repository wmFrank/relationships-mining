package task4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class PageRankReducer extends Reducer<Text, Text, Text, Text> {
    private double d;
    PageRankReducer(){
        d = 0.85;
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String ls = "";
        double pr = 0.0;
        for(Text value:values) {
            String tmp = value.toString();
            if(tmp.startsWith("$")){
                ls = ":" + tmp.substring(tmp.indexOf("$") + 1);
            }
            else {
                pr += Double.parseDouble(tmp);
            }
        }
        pr = (double)(1 - d) + d * pr;
        context.write(key, new Text(String.valueOf(pr) + ls));
    }
}
