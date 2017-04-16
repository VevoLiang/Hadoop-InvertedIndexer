package hadoop.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Vevo on 2017/4/16.
 */
public class IndexSortReducer extends Reducer<DoubleWritable,Text,Text,Text> {
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        Text newValue = new Text();
        Text newKey = new Text();
        String[] strs;
        while (it.hasNext()){
            strs = it.next().toString().split("\\t");
            newKey.set(strs[0]);
            newValue.set(strs[1]);
            context.write(newKey,newValue);
        }
    }
}
