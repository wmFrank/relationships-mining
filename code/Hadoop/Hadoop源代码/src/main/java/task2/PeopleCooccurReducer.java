package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class PeopleCooccurReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Iterator<Text> it = values.iterator();
        int totalNum = 0;
        while(it.hasNext()) {
            totalNum += Integer.parseInt(it.next().toString());
        }
        context.write(key,new Text(String.valueOf(totalNum)));
    }
}
