package task4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class GraphBuildMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String pagerank = "1.0:";
        String[] ret = value.toString().split("\t");
        String person = ret[0];
        String person_list = ret[1];
        pagerank = pagerank + person_list;
        context.write(new Text(person),new Text(pagerank));
    }
}
