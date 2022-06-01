package task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class PeopleCooccur {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "PeopleCooccur");
            job.setJarByClass(PeopleCooccur.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(PeopleCooccurMapper.class);
            job.setReducerClass(PeopleCooccurReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
