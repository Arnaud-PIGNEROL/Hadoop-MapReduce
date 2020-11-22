package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private static DoubleWritable height = null;

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // select the species of the trees
        if(!value.toString().contains("GENRE")){
            Text kind = new Text(value.toString().split(";")[2]);

            // select the height of the trees
            if(!value.toString().split(";")[6].isEmpty()){
                height = new DoubleWritable(Double.parseDouble(value.toString().split(";")[6]));
            }

            // write only if there is an height
            if(height != null)
                context.write(height, kind);
        }
    }

}
