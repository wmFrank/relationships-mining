package task5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LabelPropagationer {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "pageranker");
            job.setJarByClass(LabelPropagationer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(LabelPropagationMapper.class);
            job.setReducerClass(LabelPropagationReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
            job.waitForCompletion(true);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
