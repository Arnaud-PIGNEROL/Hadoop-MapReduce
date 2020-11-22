package com.opstty.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class PairOldest implements WritableComparable {

    private IntWritable district;
    private IntWritable age;

    public PairOldest() {
        this.district = new IntWritable();
        this.age = new IntWritable();
    }

    public PairOldest(IntWritable district, IntWritable age) {
        this.district = district;
        this.age = age;
    }

    public IntWritable getDistrict() {
        return this.district;
    }

    public IntWritable getAge() {
        return this.age;
    }

    public void set(IntWritable district, IntWritable age) {
        this.district = district;
        this.age = age;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        district.write(dataOutput);
        age.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        district.readFields(dataInput);
        age.readFields(dataInput);
    }
}
