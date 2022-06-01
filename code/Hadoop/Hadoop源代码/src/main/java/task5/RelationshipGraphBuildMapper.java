package task5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RelationshipGraphBuildMapper extends Mapper<LongWritable, Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String names = line.split("\\t")[0];
        String num = line.split("\\t")[1];
        String firstName = names.split(",")[0];
        String secondName = names.split(",")[1];

        context.write(new Text(firstName), new Text(secondName + "," + num));
    }

}
