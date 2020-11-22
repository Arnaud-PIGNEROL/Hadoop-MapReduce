package com.opstty.mapper;

import com.opstty.job.PairOldest;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class OldestTreeMapper extends Mapper<LongWritable, Text, IntWritable, PairOldest> {

    private static IntWritable age = null;
    private static PairOldest pairOldest = new PairOldest();
    private static IntWritable one = new IntWritable(1);


    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // select the district of the trees
        if(!value.toString().contains("ARRONDISSEMENT")){
            //Text district = new Text(value.toString().split(";")[1]);
            IntWritable district = new IntWritable(Integer.parseInt(value.toString().split(";")[1]));

            // select the date of plantation of the trees
            if(!value.toString().split(";")[5].isEmpty()){
                age = new IntWritable(Integer.parseInt(value.toString().split(";")[5]));
            }

            // write only if there is an age (key: "key", values: Pair(district, age) )
            if(age != null) {

                pairOldest.set(district, age);

                context.write(one, pairOldest);
            }

        }
    }
}