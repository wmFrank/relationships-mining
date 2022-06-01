package task6_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class RankViewReducer extends Reducer<NewSort, Text, Text, Text>{
    @Override
    protected void reduce(NewSort key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        while(it.hasNext()) {
            context.write(new Text(key.toString()), it.next());
        }
    }
}
