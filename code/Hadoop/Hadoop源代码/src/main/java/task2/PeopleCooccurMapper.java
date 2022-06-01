package task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;

public class PeopleCooccurMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        String rawInput = new StringBuilder(value.toString()).
                deleteCharAt(value.toString().length()-1).toString();
        String[] rawStrings = rawInput.split(" ");

        LinkedHashSet<String> oneParaNames = new LinkedHashSet<String>();
        Collections.addAll(oneParaNames,rawStrings);

        Iterator it = oneParaNames.iterator();
        while(it.hasNext()) {
            String itStr = (String)it.next();
            Iterator it2 = oneParaNames.iterator();
            while(it2.hasNext()) {
                String itStr2 = (String)it2.next();
                if(!itStr.equals(itStr2)) {
                    context.write(new Text(itStr + "," + itStr2), new Text("1"));
                }
            }
        }

    }
}
