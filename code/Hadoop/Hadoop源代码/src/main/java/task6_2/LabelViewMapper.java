package task6_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LabelViewMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] str = value.toString().split("\t");
        String person = str[0];
        String label = str[1].split(":")[0];
        context.write(new Text(label), new Text(person));
    }
}
