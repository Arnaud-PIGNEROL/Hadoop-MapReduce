package com.opstty.job;

import com.opstty.mapper.SumTreesMapper1;
import com.opstty.reducer.SumTreesReducer1;
import com.opstty.mapper.SumTreesMapper2;
import com.opstty.reducer.SumTreesReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SumTrees {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path path = new Path("out/");

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: sum <in> [<in>...] <out>");
            System.exit(2);
        }


        // 1st job : count how many trees each district contains
        Job job1 = Job.getInstance(conf, "sum");
        job1.setJarByClass(SumTrees.class);
        job1.setMapperClass(SumTreesMapper1.class);
        job1.setReducerClass(SumTreesReducer1.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job1, path);


        job1.waitForCompletion(true);

        // 2nd job : find the maximum number of trees

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(SumTrees.class);
        job2.setMapperClass(SumTreesMapper2.class);
        job2.setReducerClass(SumTreesReducer2.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(PairSumTrees.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, path);

        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
