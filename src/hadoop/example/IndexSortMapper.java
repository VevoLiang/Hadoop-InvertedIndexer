package hadoop.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Vevo on 2017/4/16.
 */
public class IndexSortMapper extends Mapper<Object,Text,DoubleWritable,Text> {
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        double count = Double.parseDouble(key.toString());
        context.write(new DoubleWritable(count),value);
    }
}
