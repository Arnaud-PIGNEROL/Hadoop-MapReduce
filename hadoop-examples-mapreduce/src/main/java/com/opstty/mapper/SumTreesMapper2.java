package com.opstty.mapper;

import com.opstty.job.PairSumTrees;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumTreesMapper2 extends Mapper<Object, Text, IntWritable, PairSumTrees> {

    private static IntWritable one = new IntWritable(1);
    private static PairSumTrees pairSumTrees = new PairSumTrees();

    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        IntWritable district = new IntWritable(Integer.parseInt(value.toString().split("\t")[0]));

        IntWritable cpt = new IntWritable(Integer.parseInt(value.toString().split("\t")[1]));

        pairSumTrees.set(district, cpt);
        context.write(one, pairSumTrees);
    }
}
