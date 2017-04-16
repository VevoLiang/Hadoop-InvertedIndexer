package hadoop.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Vevo on 2017/4/15.
 */
public class InvertedIndexMapper extends Mapper<Object,Text,Text,Text> {
    private Text word = new Text();
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String docName = (fileSplit.getPath().getName().split("\\."))[0];
        String[] words = value.toString().split("\\s");
        for(int i=0;i<words.length;i++){
            word.set(words[i]);
            context.write(word,new Text(docName+":"+1));
        }
    }
}
