package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortHeightReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

    public void reduce(DoubleWritable values, Iterable<Text> sortHeightKey, Context context)
            throws IOException, InterruptedException {

        Text result = new Text();

        for(Text val : sortHeightKey) {
            result.set(val);
            context.write(result, values);
        }

    }
}