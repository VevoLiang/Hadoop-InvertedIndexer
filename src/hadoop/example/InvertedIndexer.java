package hadoop.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import java.io.IOException;

/**
 * Created by Vevo on 2017/4/15.
 */
public class InvertedIndexer {
    public static void main(String[] args) {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path tmpPath = new Path(args[1]+"_tmp");
        Path partitionFile = new Path(args[1]+"_partition_file");
        int reduceNum = Integer.parseInt(args[2]);


        try {
            Configuration conf = new Configuration();
            InputSampler.RandomSampler<DoubleWritable,Text> sampler =
                    new InputSampler.RandomSampler<DoubleWritable,Text>(0.1,1000,10);
            TotalOrderPartitioner.setPartitionFile(conf, partitionFile);

            Job job = Job.getInstance(conf,"invert index");
            job.setJarByClass(InvertedIndexer.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //HDFS输入路径
            FileInputFormat.addInputPath(job,inputPath);
            //HDFS输出路径
            //MapReduce中不允许已存在的输出路径，因而先删除
            tmpPath.getFileSystem(conf).delete(tmpPath, true);
            FileOutputFormat.setOutputPath(job,tmpPath);

            if(job.waitForCompletion(true)){
                Job sortJob = Job.getInstance(conf,"index sort");
                sortJob.setJarByClass(InvertedIndexer.class);
                sortJob.setInputFormatClass(KeyValueTextInputFormat.class);
                sortJob.setMapperClass(IndexSortMapper.class);
                sortJob.setPartitionerClass(TotalOrderPartitioner.class);
                sortJob.setReducerClass(IndexSortReducer.class);
                sortJob.setOutputKeyClass(DoubleWritable.class);
                sortJob.setOutputValueClass(Text.class);
                sortJob.setNumReduceTasks(reduceNum);

                FileInputFormat.addInputPath(sortJob,tmpPath);
                outputPath.getFileSystem(conf).delete(outputPath, true);
                FileOutputFormat.setOutputPath(sortJob,outputPath);
                InputSampler.writePartitionFile(sortJob, sampler);

                System.exit(sortJob.waitForCompletion(true)?0:1);
            }
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
