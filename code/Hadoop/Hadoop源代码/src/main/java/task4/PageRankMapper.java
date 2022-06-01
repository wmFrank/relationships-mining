package task4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PageRankMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] ret = value.toString().split("\t");
        String person = ret[0];
        String[] segs = ret[1].split(":");
        double pr = Double.parseDouble(segs[0]);
        if(segs.length > 1){
            String[] page_list = segs[1].split(";");
            for(String page : page_list) {
                String[] parts = page.split(",");
                String man = parts[0];
                String num = parts[1];
                String prvalue = String.valueOf(pr * Double.parseDouble(num));
                context.write(new Text(man), new Text(prvalue));
            }
            context.write(new Text(person), new Text("$" + segs[1]));
        }
    }
}

