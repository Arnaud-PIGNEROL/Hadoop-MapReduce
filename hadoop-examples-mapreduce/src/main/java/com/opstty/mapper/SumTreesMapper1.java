package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumTreesMapper1 extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // select the district of the trees
        if(!value.toString().contains("ARRONDISSEMENT")){
            IntWritable district = new IntWritable(Integer.parseInt(value.toString().split(";")[1]));

            IntWritable id = new IntWritable(Integer.parseInt(value.toString().split(";")[11]));

            context.write(district, id);
        }
    }
}
