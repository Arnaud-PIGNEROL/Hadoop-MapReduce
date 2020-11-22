package com.opstty.reducer;

import com.opstty.job.PairSumTrees;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumTreesReducer2 extends Reducer<IntWritable, PairSumTrees, IntWritable, IntWritable> {

    public void reduce(IntWritable one, Iterable<PairSumTrees> pairs, Context context)
            throws IOException, InterruptedException {

        // Take the max of the given keys

        int district = 0;
        int maximum = 0;

        for(PairSumTrees pair : pairs) {
            if(pair.getID().get() > maximum) {
                maximum = pair.getID().get();
                district = pair.getDistrict().get();
            }
        }
        context.write(new IntWritable(district), new IntWritable(maximum));
    }
}
