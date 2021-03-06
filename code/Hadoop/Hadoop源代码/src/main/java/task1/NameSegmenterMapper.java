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
                if((!fileName.equals("??????01????????????.txt")) && roleName.equals("??????")) continue;
                if((!fileName.equals("??????11?????????.txt")) && roleName.equals("??????")) continue;
                toEmit.append(roleName + " ");
            }
        }
        if(!toEmit.toString().equals("")) {
            toEmit.deleteCharAt(toEmit.length()-1);

            if(fileName.equals("??????01????????????.txt"))
                mos.write("King01",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????02????????????.txt"))
                mos.write("King02",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????03?????????.txt"))
                mos.write("King03",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????04????????????.txt"))
                mos.write("King04",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????05???????????????.txt"))
                mos.write("King05",new Text(toEmit.toString()),new Text(""));

            else if(fileName.equals("??????06???????????????.txt"))
                mos.write("King06",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????07?????????.txt"))
                mos.write("King07",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????08????????????.txt"))
                mos.write("King08",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????09???????????????.txt"))
                mos.write("King09",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????10????????????.txt"))
                mos.write("King10",new Text(toEmit.toString()),new Text(""));

            else if(fileName.equals("??????11?????????.txt"))
                mos.write("King11",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????12???????????????.txt"))
                mos.write("King12",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????13?????????.txt"))
                mos.write("King13",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????14?????????.txt"))
                mos.write("King14",new Text(toEmit.toString()),new Text(""));
            else if(fileName.equals("??????15?????????.txt"))
                mos.write("King15",new Text(toEmit.toString()),new Text(""));

            //context.write(new Text(key.toString()),new Text(toEmit.toString()));
        }
    }
}
