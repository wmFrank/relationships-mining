package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.net.URI;

public class NameSegmenter {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "NameSegmenter");

            // set the names list file
            DistributedCache.addCacheFile(new URI("peoplelist/people_name_list.txt"),job.getConfiguration());

            job.setJarByClass(NameSegmenter.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(NameSegmenterMapper.class);

            job.setNumReduceTasks(0);
            //job.setReducerClass(NameSegmenterReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            MultipleOutputs.addNamedOutput(job,"King01", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King02", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King03", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King04", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King05", TextOutputFormat.class, Text.class,Text.class);

            MultipleOutputs.addNamedOutput(job,"King06", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King07", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King08", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King09", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King10", TextOutputFormat.class, Text.class,Text.class);

            MultipleOutputs.addNamedOutput(job,"King11", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King12", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King13", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King14", TextOutputFormat.class, Text.class,Text.class);
            MultipleOutputs.addNamedOutput(job,"King15", TextOutputFormat.class, Text.class,Text.class);

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
