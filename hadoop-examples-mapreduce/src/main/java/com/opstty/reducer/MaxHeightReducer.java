package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private final DoubleWritable result = new DoubleWritable();

    public void reduce(Text nbTreesKey, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int max = 0;
        for (DoubleWritable val : values) {
            if(max < val.get()) {
                max = (int) val.get();
            }
        }
        result.set(max);

        context.write(nbTreesKey, result);
    }
}