package hadoop.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Vevo on 2017/4/15.
 * 用于实现Map输出数据<<word,doc>,count>到<word,doc_count>的数据整合
 */
public class InvertedIndexCombiner extends Reducer<Text,IntWritable,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> it = values.iterator();
        StringBuilder sb = new StringBuilder();
        Integer count = 0;
        if(it.hasNext()){
            count += it.next().get();
        }
        while(it.hasNext()){
            count += it.next().get();
        }
        String[] wordDoc = key.toString().split(",");
        Text word = new Text(wordDoc[0]);
        context.write(word,new Text(wordDoc[1]+","+count.toString()));
    }
}
