package task5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LabelPropagationMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] ret = value.toString().split("\t");
        String person = ret[0];
        String[] segs = ret[1].split(":");
        String label = segs[0];
        if(segs.length > 1){
            String[] page_list = segs[1].split(";");
            for(String page : page_list) {
                String[] parts = page.split(",");
                String man = parts[0];
                String num = parts[1];
                context.write(new Text(man), new Text(label + "," + num));
            }
            context.write(new Text(person), new Text("$" + segs[1]));
        }
    }
}

