package com.opstty.job;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairSumTrees  implements WritableComparable {

    private IntWritable district;
    private IntWritable id;

    public PairSumTrees() {
        this.district = new IntWritable(0);
        this.id = new IntWritable(0);
    }

    public PairSumTrees(IntWritable district, IntWritable id) {
        this.district = district;
        this.id = id;
    }

    public IntWritable getDistrict() {
        return this.district;
    }

    public IntWritable getID() {
        return this.id;
    }

    public void set(IntWritable district, IntWritable id) {
        this.district = district;
        this.id = id;
    }


    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        district.write(dataOutput);
        id.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        district.readFields(dataInput);
        id.readFields(dataInput);
    }


    public String toString() {
        return this.district + " " + this.id;
    }
}
