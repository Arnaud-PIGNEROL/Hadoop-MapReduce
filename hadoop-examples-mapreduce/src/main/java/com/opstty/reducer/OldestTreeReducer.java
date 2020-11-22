package com.opstty.reducer;

import com.opstty.job.PairOldest;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, PairOldest, IntWritable, IntWritable> {

    private PairOldest current = new PairOldest(new IntWritable(0), new IntWritable(2020));

    public void reduce(IntWritable useless, Iterable<PairOldest> pairs, Context context)
            throws IOException, InterruptedException {

        for(PairOldest pair : pairs) {
            if(pair.getAge().get() < current.getAge().get()) {
                current.set(new IntWritable(pair.getDistrict().get()), new IntWritable(pair.getAge().get()));
            }
        }
        context.write(current.getDistrict(), current.getAge());


    }

}
