package task6_2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class LabelViewReducer extends Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String result = "";
        Iterator<Text> it = values.iterator();
        while(it.hasNext()) {
            result += it.next().toString() + ",";
        }
        result = result.substring(0,result.length() - 1);
        context.write(key, new Text(result));
    }
}
