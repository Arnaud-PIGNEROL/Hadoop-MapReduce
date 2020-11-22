package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class SumTreesReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    // Give the count of trees of each district

    public void reduce(IntWritable district, Iterable<IntWritable> ids, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable val : ids) {
            sum++;
        }
        context.write(district, new IntWritable(sum));

    }
}
