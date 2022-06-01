package task1;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.DicLibrary;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

public class NameSegmenterMapper extends Mapper<LongWritable, Text, Text, Text> {

    private MultipleOutputs<Text,Text> mos;

    @Override
    public void setup(Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        Path[] path = DistributedCache.getLocalCacheFiles(conf);
        BufferedReader reader = null;
        reader = new BufferedReader(new FileReader(path[0].toString()));
        String line = null;

        while((line = reader.readLine()) != null) {

            DicLibrary.insert(DicLibrary.DEFAULT,line);
        }
        reader.close();

        mos = new MultipleOutputs<Text,Text>(context);
    }

    @Override
    public void cleanup(Context context)
            throws IOException,InterruptedException {
        mos.close();
        super.cleanup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context)
        throws IOException, InterruptedException {

        // get the name of the file
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        // parse text into terms
        Result result = DicAnalysis.parse(value.toString());
        List<Term> terms = result.getTerms();

        StringBuilder toEmit = new StringBuilder();
        for(Term te : terms) {
            if(te.getNatureStr().equals("userDefine")) {
                String roleName = te.getName();
                if((!fileName.equals("金庸01飞狐外传.txt")) && roleName.equals("大汉")) continue;
                if((!fileName.equals("金庸11侠客行.txt")) && roleName.equals("汉子")) continue;
                toEmit.append(roleName + " ");
            }
        }
        if(!toEmit.toString().equals("")) {
            toEmit.deleteCharAt(toEmit.length()-1);

            if(fileName.equals("金庸01飞狐外传.txt"))
                mos.write("King01",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸02雪山飞狐.txt"))
                mos.write("King02",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸03连城诀.txt"))
                mos.write("King03",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸04天龙八部.txt"))
                mos.write("King04",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸05射雕英雄传.txt"))
                mos.write("King05",new Text(toEmit.toString()),new Text(""));

            else if(fileName.equals("金庸06白马啸西风.txt"))
                mos.write("King06",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸07鹿鼎记.txt"))
                mos.write("King07",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸08笑傲江湖.txt"))
                mos.write("King08",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸09书剑恩仇录.txt"))
                mos.write("King09",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸10神雕侠侣.txt"))
                mos.write("King10",new Text(toEmit.toString()),new Text(""));

            else if(fileName.equals("金庸11侠客行.txt"))
                mos.write("King11",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸12倚天屠龙记.txt"))
                mos.write("King12",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸13碧血剑.txt"))
                mos.write("King13",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸14鸳鸯刀.txt"))
                mos.write("King14",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("金庸15越女剑.txt"))
                mos.write("King15",new Text(toEmit.toString()),new Text(""));

            //context.write(new Text(key.toString()),new Text(toEmit.toString()));
        }
    }
}
