package hadoop.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Vevo on 2017/4/15.
 */
public class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String,Integer> map = new HashMap<String,Integer>();
        Iterator<Text> it = values.iterator();
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

        Iterator<Integer> valueIterator = map.values().iterator();
        long count = 0L;
        double average = 0;
        while(valueIterator.hasNext()){
            count += valueIterator.next();
        }
        average = (double)count/map.size();

        StringBuilder docIndex = new StringBuilder();
        Iterator<Map.Entry<String,Integer>> entryIterator =map.entrySet().iterator();
        Map.Entry<String,Integer> entry;
        if(entryIterator.hasNext()){
            entry = entryIterator.next();
            docIndex.append(entry.getKey()+":"+entry.getValue());
        }
        while(entryIterator.hasNext()){
            docIndex.append(";");
            entry = entryIterator.next();
            docIndex.append(entry.getKey()+":"+entry.getValue());
        }
        context.write(key,new Text(average+","+docIndex.toString()));
    }
}
