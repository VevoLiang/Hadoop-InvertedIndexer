package hadoop.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Vevo on 2017/4/15.
 */
public class InvertedIndexReducer extends Reducer<Text,Text,DoubleWritable,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String,Integer> map = new HashMap<String,Integer>();
        Iterator<Text> it = values.iterator();

        //item[0]用于保存词语，itemp[1]用于保存频数（Map传递过来的一般是1）
        String[] item;
        if(it.hasNext()){
            item = it.next().toString().split(":");
            map.put(item[0],1);
        }
        while(it.hasNext()){
            item = it.next().toString().split(":");
            if(map.containsKey(item[0])){
                map.put(item[0],((int)map.get(item[0]))+1);
            }else{
                map.put(item[0],1);
            }
        }

        //计算词语平均出现次数
        Iterator<Integer> valueIterator = map.values().iterator();
        long count = 0L;
        double average = 0L;
        while(valueIterator.hasNext()){
            count += valueIterator.next();
        }
        average = (double)count/map.size();

        StringBuilder docIndex = new StringBuilder();
        Iterator<Map.Entry<String,Integer>> entryIterator =map.entrySet().iterator();
        Map.Entry<String,Integer> entry;
        if(entryIterator.hasNext()){
            entry = entryIterator.next();
            docIndex.append(entry.getKey()).append(":").append(entry.getValue());
        }
        while(entryIterator.hasNext()){
            docIndex.append(";");
            entry = entryIterator.next();
            docIndex.append(entry.getKey()).append(":").append(entry.getValue());
        }
        context.write(new DoubleWritable(average),new Text(key.toString() + "\t" + average+","+docIndex.toString()));
    }
}
