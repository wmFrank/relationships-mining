package task6_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankViewMapper extends Mapper<LongWritable, Text, NewSort, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] str = value.toString().split("\t");
        Text page = new Text(str[0]);
        NewSort pr = new NewSort(Double.parseDouble(str[1].split(":")[0]));
        context.write(pr, page);
    }
}
