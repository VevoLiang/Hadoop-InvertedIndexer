package hadoop.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Vevo on 2017/4/16.
 */
public class IndexSortReducer extends Reducer<DoubleWritable,Text,Text,Text> {
    private static Configuration config;
    private static HBaseAdmin hAdmin;
    private static HTable wuxia = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum","localhost");
        config.set("hbbase.zookeeper.property.clientPort","2181");
        hAdmin = new HBaseAdmin(config);
        wuxia = new HTable(config,"wuxia");
        wuxia.setAutoFlush(false);
    }

    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        Text newValue = new Text();
        Text newKey = new Text();

        List<Put> putList = new ArrayList<Put>();
        Put put;
        String[] strs;
        while (it.hasNext()){
            strs = it.next().toString().split("\\t");

            if(strs[0].length()!=0){
                put = new Put(Bytes.toBytes(strs[0]));
                put.addColumn(Bytes.toBytes("count"),Bytes.toBytes("average"),Bytes.toBytes(key.toString()));
                wuxia.put(put);
            }

            newKey.set(strs[0]);
            newValue.set(strs[1]);
            context.write(newKey,newValue);
        }
        wuxia.flushCommits();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        wuxia.close();
        hAdmin.close();
    }
}
