# Lab MapReduce2 - Java

## 1.6.3 - Run the Job
```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar wordcount input-trees/* ResultTrees

20/11/03 08:54:58 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/03 08:54:59 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604390099021, maxDate=1604994899021, sequenceNumber=3248, masterKeyId=38 on ha-hdfs:efrei
20/11/03 08:54:59 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604390099021, maxDate=1604994899021, sequenceNumber=3248, masterKeyId=38)
20/11/03 08:54:59 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_1194
20/11/03 08:55:00 INFO input.FileInputFormat: Total input files to process : 1
20/11/03 08:55:00 INFO mapreduce.JobSubmitter: number of splits:1
20/11/03 08:55:00 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_1194
20/11/03 08:55:00 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604390099021, maxDate=1604994899021, sequenceNumber=3248, masterKeyId=38)]
20/11/03 08:55:00 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/03 08:55:00 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/03 08:55:01 INFO impl.YarnClientImpl: Submitted application application_1603290159664_1194
20/11/03 08:55:01 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_1194/
20/11/03 08:55:01 INFO mapreduce.Job: Running job: job_1603290159664_1194
20/11/03 08:55:11 INFO mapreduce.Job: Job job_1603290159664_1194 running in uber mode : false
20/11/03 08:55:11 INFO mapreduce.Job:  map 0% reduce 0%
ça 20/11/03 08:55:20 INFO mapreduce.Job:  map 100% reduce 0%
20/11/03 08:55:25 INFO mapreduce.Job:  map 100% reduce 100%
20/11/03 08:55:25 INFO mapreduce.Job: Job job_1603290159664_1194 completed successfully
20/11/03 08:55:25 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=16561
                FILE: Number of bytes written=526317
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=14251
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=19992
                Total time spent by all reduces in occupied slots (ms)=12064
                Total time spent by all map tasks (ms)=6664
                Total time spent by all reduce tasks (ms)=3016
                Total vcore-milliseconds taken by all map tasks=6664
                Total vcore-milliseconds taken by all reduce tasks=3016
                Total megabyte-milliseconds taken by all map tasks=10235904
                Total megabyte-milliseconds taken by all reduce tasks=6176768
        Map-Reduce Framework
                Map input records=98
                Map output records=1219
                Map output bytes=21556
                Map output materialized bytes=16561
                Input split bytes=114
                Combine input records=1219
                Combine output records=579
                Reduce input groups=579
                Reduce shuffle bytes=16561
                Reduce input records=579
                Reduce output records=579
                Spilled Records=1158
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=214
                CPU time spent (ms)=3870
                Physical memory (bytes) snapshot=1452941312
                Virtual memory (bytes) snapshot=7266541568
                Total committed heap usage (bytes)=1556611072
                Peak Map Physical memory (bytes)=1160609792
                Peak Map Virtual memory (bytes)=3395493888
                Peak Reduce Physical memory (bytes)=292331520
                Peak Reduce Virtual memory (bytes)=3871047680
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=14251

-sh-4.2$ hdfs dfs -ls
Found 10 items
drwx------   - apignerol hdfs          0 2020-10-20 02:00 .Trash
drwx------   - apignerol hdfs          0 2020-11-03 08:55 .staging
drwxr-xr-x   - apignerol hdfs          0 2020-10-19 10:48 QuasiMonteCarlo_1603097327692_474589705
drwxr-xr-x   - apignerol hdfs          0 2020-11-03 08:55 ResultTrees
drwxr-xr-x   - apignerol hdfs          0 2020-10-19 11:02 data
-rw-r--r--   1 apignerol hdfs    1276201 2020-10-19 10:42 davinci.txt
drwxr-xr-x   - apignerol hdfs          0 2020-10-19 11:45 gutenberg
drwxr-xr-x   - apignerol hdfs          0 2020-10-19 15:02 gutenberg-output
drwxr-xr-x   - apignerol hdfs          0 2020-11-03 08:54 input-trees
drwxr-xr-x   - apignerol hdfs          0 2020-10-19 10:43 wordcount


-sh-4.2$ hdfs dfs -ls input-trees
Found 1 items
-rw-r--r--   3 apignerol hdfs      16778 2020-11-03 08:54 input-trees/trees.csv



-sh-4.2$ hdfs dfs -cat /user/apignerol/ResultTrees/part-r-00000

...
l'Atlas;Glauca;24;Parc  1
la      36
lac     7
laricio;Pinaceae;1882;30.0;240.0;All‚e  1
liège;;32;Jardin        1
minimes)        1
neuf)   1
noir;;69;Bois   1
noir;;85;Bois   1
noir;Austriaca;97;Bois  1
orme    3
papier;;33;Parc 1
pendula;62;Square       1
ple;Glauca      1
pleureur;Pendula;20;Bois        1
pleureur;Pendula;63;Bois        1
pleureur;Pendula;70;Bois        1
pochettes;;71;Bois      1
pourpre;Purpurea;18;Bois        1
pourpre;Purpurea;25;Parc        1
pourpre;Purpurea;38;Bois        1
pourpre;Purpurea;60;Stade       1
quarante        5
repos,  2
ronde   1
route   14
rouve;;80;Bois  1
rouvre;;19;Bois 1
rue     43
sempervirent;;27;Parc   1
singe;;39;Bois  1
vert;;98;Bois   1
à       13
écus;;10;Parc   1
écus;;31;Jardin 1
écus;;46;Parc   1
écus;;64;Bois   1
écus;;84;Bois   1
île     1
```

## 1.8 - Remarkable trees of Paris


### 1.8.1 - Districts containing trees
#### we create 3 new classes and update the AppDriver
#### District.java
```Java
package com.opstty.job;

import com.opstty.mapper.DistrictsMapper;
import com.opstty.reducer.DistrictsReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class District {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: district <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "district");
        job.setJarByClass(District.class);
        job.setMapperClass(DistrictsMapper.class);
        job.setCombinerClass(DistrictsReducer.class);
        job.setReducerClass(DistrictsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### DistrictsMapper.java
```Java
package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DistrictsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(!value.toString().contains("ARRONDISSEMENT")){
            Text district = new Text(value.toString().split(";")[1]);
            context.write(district, NullWritable.get());
        }
    }
}
```

#### DistrictsReducer.java
```Java
package com.opstty.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictsReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    public void reduce(Text districtKey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(districtKey, NullWritable.get());
    }
}
```

#### Appdriver
```Java
package com.opstty;

import com.opstty.job.District;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```


#### If there is an error like
```CMD
Unknown program 'district' chosen.
Valid program names are:
  wordcount: A map/reduce program that counts the words in the input files.
```
#### We have to reinstall the package (At the right-hand corner, click on Maven the Package) on IntellijIdea and refresh FileZila


#### We obtain this result
```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar district input-trees/* Districts

20/11/03 10:10:36 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/03 10:10:36 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604394636667, maxDate=1604999436667, sequenceNumber=3336, masterKeyId=38 on ha-hdfs:efrei
20/11/03 10:10:36 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604394636667, maxDate=1604999436667, sequenceNumber=3336, masterKeyId=38)
20/11/03 10:10:36 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_1279
20/11/03 10:10:37 INFO input.FileInputFormat: Total input files to process : 1
20/11/03 10:10:37 INFO mapreduce.JobSubmitter: number of splits:1
20/11/03 10:10:37 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_1279
20/11/03 10:10:37 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604394636667, maxDate=1604999436667, sequenceNumber=3336, masterKeyId=38)]
20/11/03 10:10:38 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/03 10:10:38 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/03 10:10:39 INFO impl.YarnClientImpl: Submitted application application_1603290159664_1279
20/11/03 10:10:39 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_1279/
20/11/03 10:10:39 INFO mapreduce.Job: Running job: job_1603290159664_1279
20/11/03 10:10:49 INFO mapreduce.Job: Job job_1603290159664_1279 running in uber mode : false
20/11/03 10:10:49 INFO mapreduce.Job:  map 0% reduce 0%
20/11/03 10:10:58 INFO mapreduce.Job:  map 100% reduce 0%
20/11/03 10:11:04 INFO mapreduce.Job:  map 100% reduce 100%
20/11/03 10:11:04 INFO mapreduce.Job: Job job_1603290159664_1279 completed successfully
20/11/03 10:11:04 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=477
                FILE: Number of bytes written=494157
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=277
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=20277
                Total time spent by all reduces in occupied slots (ms)=11816
                Total time spent by all map tasks (ms)=6759
                Total time spent by all reduce tasks (ms)=2954
                Total vcore-milliseconds taken by all map tasks=6759
                Total vcore-milliseconds taken by all reduce tasks=2954
                Total megabyte-milliseconds taken by all map tasks=10381824
                Total megabyte-milliseconds taken by all reduce tasks=6049792
        Map-Reduce Framework
                Map input records=98
                Map output records=97
                Map output bytes=277
                Map output materialized bytes=477
                Input split bytes=114
                Combine input records=97
                Combine output records=97
                Reduce input groups=17
                Reduce shuffle bytes=477
                Reduce input records=97
                Reduce output records=97
                Spilled Records=194
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=192
                CPU time spent (ms)=2960
                Physical memory (bytes) snapshot=1455534080
                Virtual memory (bytes) snapshot=7267213312
                Total committed heap usage (bytes)=1562378240
                Peak Map Physical memory (bytes)=1158148096
                Peak Map Virtual memory (bytes)=3393798144
                Peak Reduce Physical memory (bytes)=297385984
                Peak Reduce Virtual memory (bytes)=3873415168
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=277

hdfs dfs -cat /user/apignerol/Districts/part-r-00000

11
12
13
14
15
16
17
18
19
20
3
4
5
6
7
8
9
```

### 1.8.2 - Show all existing species
#### As before we have to create 3 new class and update the AppDriver one

#### Species.java
```Java
package com.opstty.job;

import com.opstty.mapper.SpeciesMapper;
import com.opstty.reducer.SpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Species {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: species <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "species");
        job.setJarByClass(Species.class);
        job.setMapperClass(SpeciesMapper.class);
        job.setCombinerClass(SpeciesReducer.class);
        job.setReducerClass(SpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### SpeciesMapper.java
```Java
package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("ESPECE")) {
            Text species = new Text(value.toString().split(";")[3]);
            context.write(species, NullWritable.get());
        }
    }
}
```

#### SpeciesReducer.java
```Java
package com.opstty.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SpeciesReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    public void reduce(Text speciesKey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(speciesKey, NullWritable.get());
    }
}
```

#### AppDriver.java
```Java
package com.opstty;

import com.opstty.job.District;
import com.opstty.job.Species;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```



```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar species input-trees/* Species

20/11/03 10:58:05 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/03 10:58:05 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604397485586, maxDate=1605002285586, sequenceNumber=3415, masterKeyId=38 on ha-hdfs:efrei
20/11/03 10:58:05 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604397485586, maxDate=1605002285586, sequenceNumber=3415, masterKeyId=38)
20/11/03 10:58:05 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_1344
20/11/03 10:58:06 INFO input.FileInputFormat: Total input files to process : 1
20/11/03 10:58:06 INFO mapreduce.JobSubmitter: number of splits:1
20/11/03 10:58:06 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_1344
20/11/03 10:58:06 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604397485586, maxDate=1605002285586, sequenceNumber=3415, masterKeyId=38)]
20/11/03 10:58:07 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/03 10:58:07 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/03 10:58:07 INFO impl.YarnClientImpl: Submitted application application_1603290159664_1344
20/11/03 10:58:07 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_1344/
20/11/03 10:58:07 INFO mapreduce.Job: Running job: job_1603290159664_1344
20/11/03 10:58:18 INFO mapreduce.Job: Job job_1603290159664_1344 running in uber mode : false
20/11/03 10:58:18 INFO mapreduce.Job:  map 0% reduce 0%
20/11/03 10:58:27 INFO mapreduce.Job:  map 100% reduce 0%
20/11/03 10:58:38 INFO mapreduce.Job:  map 100% reduce 100%
20/11/03 10:58:38 INFO mapreduce.Job: Job job_1603290159664_1344 completed successfully
20/11/03 10:58:38 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=84
                FILE: Number of bytes written=493367
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=44
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=20541
                Total time spent by all reduces in occupied slots (ms)=28648
                Total time spent by all map tasks (ms)=6847
                Total time spent by all reduce tasks (ms)=7162
                Total vcore-milliseconds taken by all map tasks=6847
                Total vcore-milliseconds taken by all reduce tasks=7162
                Total megabyte-milliseconds taken by all map tasks=10516992
                Total megabyte-milliseconds taken by all reduce tasks=14667776
        Map-Reduce Framework
                Map input records=98
                Map output records=97
                Map output bytes=277
                Map output materialized bytes=84
                Input split bytes=114
                Combine input records=97
                Combine output records=17
                Reduce input groups=17
                Reduce shuffle bytes=84
                Reduce input records=17
                Reduce output records=17
                Spilled Records=34
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=200
                CPU time spent (ms)=3300
                Physical memory (bytes) snapshot=1449086976
                Virtual memory (bytes) snapshot=7268339712
                Total committed heap usage (bytes)=1560805376
                Peak Map Physical memory (bytes)=1155690496
                Peak Map Virtual memory (bytes)=3394256896
                Peak Reduce Physical memory (bytes)=293396480
                Peak Reduce Virtual memory (bytes)=3874082816
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=44



-sh-4.2$ hdfs dfs -cat /user/apignerol/Species/part-r-00000
araucana
atlantica
australis
baccata
bignonioides
biloba
bungeana
cappadocicum
carpinifolia
colurna
coulteri
decurrens
dioicus
distichum
excelsior
fraxinifolia
giganteum
giraldii
glutinosa
grandiflora
hippocastanum
ilex
involucrata
japonicum
kaki
libanii
monspessulanum
nigra
nigra laricio
opalus
orientalis
papyrifera
petraea
pomifera
pseudoacacia
sempervirens
serrata
stenoptera
suber
sylvatica
tomentosa
tulipifera
ulmoides
virginiana
x acerifolia
```

### 1.8.3 - Number of trees by species
#### NbTreesBySpecies.java
```Java
package com.opstty.job;

import com.opstty.mapper.NbTreesBySpeciesMapper;
import com.opstty.reducer.NbTreesBySpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NbTreesBySpecies {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: nbTrees <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "nbTrees");
        job.setJarByClass(Species.class);
        job.setMapperClass(NbTreesBySpeciesMapper.class);
        job.setCombinerClass(NbTreesBySpeciesReducer.class);
        job.setReducerClass(NbTreesBySpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### NbTreesBySpeciesMapper.java
```Java
package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class NbTreesBySpeciesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().contains("GENRE")) {

            StringTokenizer itr = new StringTokenizer(value.toString().split(";")[2]);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }
}
```

#### NbTreesBySpeciesReducer.java
```Java
package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NbTreesBySpeciesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable result = new IntWritable();

    public void reduce(Text nbTreesKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);

        context.write(nbTreesKey, result);
    }
}
```

#### AppDriver.java
```Java
package com.opstty;

import com.opstty.job.District;
import com.opstty.job.NbTreesBySpecies;
import com.opstty.job.Species;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("nbTrees", NbTreesBySpecies.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```

#### Result
```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar nbTrees input-trees/* NbTrees

20/11/03 12:50:11 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/03 12:50:11 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604404211604, maxDate=1605009011604, sequenceNumber=3499, masterKeyId=38 on ha-hdfs:efrei
20/11/03 12:50:11 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604404211604, maxDate=1605009011604, sequenceNumber=3499, masterKeyId=38)
20/11/03 12:50:11 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_1416
20/11/03 12:50:12 INFO input.FileInputFormat: Total input files to process : 1
20/11/03 12:50:12 INFO mapreduce.JobSubmitter: number of splits:1
20/11/03 12:50:12 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_1416
20/11/03 12:50:12 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604404211604, maxDate=1605009011604, sequenceNumber=3499, masterKeyId=38)]
20/11/03 12:50:12 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/03 12:50:13 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/03 12:50:13 INFO impl.YarnClientImpl: Submitted application application_1603290159664_1416
20/11/03 12:50:13 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_1416/
20/11/03 12:50:13 INFO mapreduce.Job: Running job: job_1603290159664_1416
20/11/03 12:50:23 INFO mapreduce.Job: Job job_1603290159664_1416 running in uber mode : false
20/11/03 12:50:23 INFO mapreduce.Job:  map 0% reduce 0%
20/11/03 12:50:32 INFO mapreduce.Job:  map 100% reduce 0%
20/11/03 12:50:41 INFO mapreduce.Job:  map 100% reduce 100%
20/11/03 12:50:42 INFO mapreduce.Job: Job job_1603290159664_1416 completed successfully
20/11/03 12:50:43 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=727
                FILE: Number of bytes written=494691
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=539
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=19440
                Total time spent by all reduces in occupied slots (ms)=28988
                Total time spent by all map tasks (ms)=6480
                Total time spent by all reduce tasks (ms)=7247
                Total vcore-milliseconds taken by all map tasks=6480
                Total vcore-milliseconds taken by all reduce tasks=7247
                Total megabyte-milliseconds taken by all map tasks=9953280
                Total megabyte-milliseconds taken by all reduce tasks=14841856
        Map-Reduce Framework
                Map input records=98
                Map output records=109
                Map output bytes=1431
                Map output materialized bytes=727
                Input split bytes=114
                Combine input records=109
                Combine output records=46
                Reduce input groups=46
                Reduce shuffle bytes=727
                Reduce input records=46
                Reduce output records=46
                Spilled Records=92
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=172
                CPU time spent (ms)=3380
                Physical memory (bytes) snapshot=1447501824
                Virtual memory (bytes) snapshot=7268868096
                Total committed heap usage (bytes)=1564999680
                Peak Map Physical memory (bytes)=1156866048
                Peak Map Virtual memory (bytes)=3394183168
                Peak Reduce Physical memory (bytes)=290635776
                Peak Reduce Virtual memory (bytes)=3874684928
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=539



-sh-4.2$ hdfs dfs -cat /user/apignerol/NbTrees/part-r-00000
Acer    3
Aesculus        3
Ailanthus       1
Alnus   1
Araucaria       1
Broussonetia    1
Calocedrus      1
Catalpa 1
Cedrus  4
Celtis  1
Corylus 3
Davidia 1
Diospyros       4
Eucommia        1
Fagus   8
Fraxinus        1
Ginkgo  5
Gymnocladus     1
Juglans 1
Liriodendron    2
Maclura 1
Magnolia        1
Paulownia       1
Pinus   5
Platanus        19
Pterocarya      3
Quercus 4
Robinia 1
Sequoia 1
Sequoiadendron  5
Styphnolobium   1
Taxodium        3
Taxus   2
Tilia   1
Ulmus   1
Zelkova 4
```

### 1.8.4 - Maximum height per specie of tree
#### MaxHeight.java
```Java
package com.opstty.job;

import com.opstty.mapper.MaxHeightMapper;
import com.opstty.reducer.MaxHeightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxHeight {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: maxHeight <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "maxH");
        job.setJarByClass(MaxHeight.class);
        job.setMapperClass(MaxHeightMapper.class);
        job.setCombinerClass(MaxHeightReducer.class);
        job.setReducerClass(MaxHeightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### MaxHeightMapper.java
```Java
package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
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
                context.write(kind,height);
        }
    }

}
```

#### MaxHeightReducer.java
```Java
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
```

#### AppDriver.java
```Java
package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("maxH", MaxHeight.class,
                    "A map/reduce program that shows the max height for each species of tree in Paris.");

            programDriver.addClass("nbTrees", NbTreesBySpecies.class,
                    "A map/reduce program that counts the number of trees by species in Paris.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```

#### Result
```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar maxH input-trees/* MaxHeight

20/11/04 19:19:06 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/04 19:19:06 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604513946414, maxDate=1605118746414, sequenceNumber=4276, masterKeyId=40 on ha-hdfs:efrei
20/11/04 19:19:06 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604513946414, maxDate=1605118746414, sequenceNumber=4276, masterKeyId=40)
20/11/04 19:19:06 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_2112
20/11/04 19:19:07 INFO input.FileInputFormat: Total input files to process : 1
20/11/04 19:19:07 INFO mapreduce.JobSubmitter: number of splits:1
20/11/04 19:19:07 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_2112
20/11/04 19:19:07 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604513946414, maxDate=1605118746414, sequenceNumber=4276, masterKeyId=40)]
20/11/04 19:19:07 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/04 19:19:07 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/04 19:19:08 INFO impl.YarnClientImpl: Submitted application application_1603290159664_2112
20/11/04 19:19:08 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_2112/
20/11/04 19:19:08 INFO mapreduce.Job: Running job: job_1603290159664_2112
20/11/04 19:19:18 INFO mapreduce.Job: Job job_1603290159664_2112 running in uber mode : false
20/11/04 19:19:18 INFO mapreduce.Job:  map 0% reduce 0%
20/11/04 19:19:27 INFO mapreduce.Job:  map 100% reduce 0%
20/11/04 19:19:37 INFO mapreduce.Job:  map 100% reduce 100%
20/11/04 19:19:37 INFO mapreduce.Job: Job job_1603290159664_2112 completed successfully
20/11/04 19:19:37 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=683
                FILE: Number of bytes written=494565
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=496
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=20331
                Total time spent by all reduces in occupied slots (ms)=26724
                Total time spent by all map tasks (ms)=6777
                Total time spent by all reduce tasks (ms)=6681
                Total vcore-milliseconds taken by all map tasks=6777
                Total vcore-milliseconds taken by all reduce tasks=6681
                Total megabyte-milliseconds taken by all map tasks=10409472
                Total megabyte-milliseconds taken by all reduce tasks=13682688
        Map-Reduce Framework
                Map input records=98
                Map output records=97
                Map output bytes=1611
                Map output materialized bytes=683
                Input split bytes=114
                Combine input records=97
                Combine output records=36
                Reduce input groups=36
                Reduce shuffle bytes=683
                Reduce input records=36
                Reduce output records=36
                Spilled Records=72
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=216
                CPU time spent (ms)=3400
                Physical memory (bytes) snapshot=1453481984
                Virtual memory (bytes) snapshot=7272906752
                Total committed heap usage (bytes)=1542979584
                Peak Map Physical memory (bytes)=1161469952
                Peak Map Virtual memory (bytes)=3399700480
                Peak Reduce Physical memory (bytes)=292012032
                Peak Reduce Virtual memory (bytes)=3873206272
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=496



-sh-4.2$ hdfs dfs -cat /user/apignerol/MaxHeight/part-r-00000

Acer    16.0
Aesculus        30.0
Ailanthus       35.0
Alnus   16.0
Araucaria       9.0
Broussonetia    12.0
Calocedrus      20.0
Catalpa 15.0
Cedrus  30.0
Celtis  16.0
Corylus 20.0
Davidia 12.0
Diospyros       14.0
Eucommia        12.0
Fagus   30.0
Fraxinus        30.0
Ginkgo  33.0
Gymnocladus     10.0
Juglans 28.0
Liriodendron    35.0
Maclura 13.0
Magnolia        12.0
Paulownia       20.0
Pinus   30.0
Platanus        45.0
Pterocarya      30.0
Quercus 31.0
Robinia 11.0
Sequoia 30.0
Sequoiadendron  35.0
Styphnolobium   10.0
Taxodium        35.0
Taxus   13.0
Tilia   20.0
Ulmus   15.0
Zelkova 30.0
```

### 1.8.5 - Sort the tree height from the smallest to the largest

#### SortHeight.java
```Java
package com.opstty.job;

import com.opstty.mapper.SortHeightMapper;
import com.opstty.reducer.SortHeightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SortHeight {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: sortHeight <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "sortH");
        job.setJarByClass(SortHeight.class);
        job.setMapperClass(SortHeightMapper.class);
        job.setReducerClass(SortHeightReducer.class);

        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);


        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### SortHeightMapper.java
```Java
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
```

#### SortHeightReducer.java
```Java
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
```

#### AppDriver.java
```Java
package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("sortH", SortHeight.class,
                    "A map/reduce program that shows the height for each species of tree in Paris sorted from the smallest to the largest.");

            programDriver.addClass("maxH", MaxHeight.class,
                    "A map/reduce program that shows the max height for each species of tree in Paris.");

            programDriver.addClass("nbTrees", NbTreesBySpecies.class,
                    "A map/reduce program that counts the number of trees by species in Paris.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```

#### Result
```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar sortH input-trees/* SortHeight

20/11/04 20:15:19 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/04 20:15:19 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604517319512, maxDate=1605122119512, sequenceNumber=4282, masterKeyId=40 on ha-hdfs:efrei
20/11/04 20:15:19 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604517319512, maxDate=1605122119512, sequenceNumber=4282, masterKeyId=40)
20/11/04 20:15:19 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_2116
20/11/04 20:15:20 INFO input.FileInputFormat: Total input files to process : 1
20/11/04 20:15:20 INFO mapreduce.JobSubmitter: number of splits:1
20/11/04 20:15:20 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_2116
20/11/04 20:15:20 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1604517319512, maxDate=1605122119512, sequenceNumber=4282, masterKeyId=40)]
20/11/04 20:15:20 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/04 20:15:20 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/04 20:15:21 INFO impl.YarnClientImpl: Submitted application application_1603290159664_2116
20/11/04 20:15:21 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_2116/
20/11/04 20:15:21 INFO mapreduce.Job: Running job: job_1603290159664_2116
20/11/04 20:15:31 INFO mapreduce.Job: Job job_1603290159664_2116 running in uber mode : false
20/11/04 20:15:31 INFO mapreduce.Job:  map 0% reduce 0%
20/11/04 20:15:40 INFO mapreduce.Job:  map 100% reduce 0%
20/11/04 20:15:49 INFO mapreduce.Job:  map 100% reduce 100%
20/11/04 20:15:50 INFO mapreduce.Job: Job job_1603290159664_2116 completed successfully
20/11/04 20:15:50 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=683
                FILE: Number of bytes written=494567
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=496
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=20682
                Total time spent by all reduces in occupied slots (ms)=26556
                Total time spent by all map tasks (ms)=6894
                Total time spent by all reduce tasks (ms)=6639
                Total vcore-milliseconds taken by all map tasks=6894
                Total vcore-milliseconds taken by all reduce tasks=6639
                Total megabyte-milliseconds taken by all map tasks=10589184
                Total megabyte-milliseconds taken by all reduce tasks=13596672
        Map-Reduce Framework
                Map input records=98
                Map output records=97
                Map output bytes=1611
                Map output materialized bytes=683
                Input split bytes=114
                Combine input records=97
                Combine output records=36
                Reduce input groups=36
                Reduce shuffle bytes=683
                Reduce input records=36
                Reduce output records=36
                Spilled Records=72
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=186
                CPU time spent (ms)=3140
                Physical memory (bytes) snapshot=1451855872
                Virtual memory (bytes) snapshot=7271174144
                Total committed heap usage (bytes)=1555562496
                Peak Map Physical memory (bytes)=1161560064
                Peak Map Virtual memory (bytes)=3396083712
                Peak Reduce Physical memory (bytes)=290295808
                Peak Reduce Virtual memory (bytes)=3875090432
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=496




-sh-4.2$ hdfs dfs -cat /user/apignerol/SortHeight/part-r-00000

Fagus   2.0
Taxus   5.0
Cedrus  6.0
Araucaria       9.0
Styphnolobium   10.0
Quercus 10.0
Pinus   10.0
Gymnocladus     10.0
Fagus   10.0
Robinia 11.0
Diospyros       12.0
Magnolia        12.0
Zelkova 12.0
Eucommia        12.0
Acer    12.0
Diospyros       12.0
Broussonetia    12.0
Davidia 12.0
Taxus   13.0
Maclura 13.0
Diospyros       14.0
Pinus   14.0
Diospyros       14.0
Acer    15.0
Catalpa 15.0
Fagus   15.0
Ulmus   15.0
Quercus 15.0
Alnus   16.0
Acer    16.0
Zelkova 16.0
Celtis  16.0
Ginkgo  18.0
Zelkova 18.0
Aesculus        18.0
Fagus   18.0
Corylus 20.0
Platanus        20.0
Tilia   20.0
Corylus 20.0
Calocedrus      20.0
Corylus 20.0
Platanus        20.0
Fagus   20.0
Paulownia       20.0
Sequoiadendron  20.0
Taxodium        20.0
Platanus        20.0
Liriodendron    22.0
Ginkgo  22.0
Aesculus        22.0
Platanus        22.0
Pterocarya      22.0
Fagus   23.0
Platanus        25.0
Ginkgo  25.0
Cedrus  25.0
Platanus        25.0
Ginkgo  25.0
Pinus   25.0
Platanus        26.0
Platanus        27.0
Pterocarya      27.0
Sequoiadendron  27.0
Juglans 28.0
Zelkova 30.0
Cedrus  30.0
Sequoia 30.0
Fagus   30.0
Platanus        30.0
Cedrus  30.0
Quercus 30.0
Pinus   30.0
Taxodium        30.0
Aesculus        30.0
Platanus        30.0
Pinus   30.0
Fagus   30.0
Sequoiadendron  30.0
Fraxinus        30.0
Sequoiadendron  30.0
Pterocarya      30.0
Platanus        31.0
Quercus 31.0
Platanus        32.0
Ginkgo  33.0
Platanus        34.0
Sequoiadendron  35.0
Liriodendron    35.0
Taxodium        35.0
Ailanthus       35.0
Platanus        35.0
Platanus        40.0
Platanus        40.0
Platanus        40.0
Platanus        42.0
```


### 1.8.6 - District containing the oldest tree

#### OldestTree.java
```Java
package com.opstty.job;

import com.opstty.mapper.OldestTreeMapper;
import com.opstty.reducer.OldestTreeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OldestTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: oldest <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "oldest");
        job.setJarByClass(OldestTree.class);
        job.setMapperClass(OldestTreeMapper.class);
        job.setReducerClass(OldestTreeReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(PairOldest.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### OldestTreeMapper.java
```Java
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
```

#### OldestTreeReducer.java
```Java
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
                context.write(current.getDistrict(), current.getAge());
            }
        }


    }

}
```


#### PairsOldest.java
```Java
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
```


#### AppDriver.java
```Java
package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("oldest", OldestTree.class,
                    "A map/reduce program that shows the oldest tree and its district.");

            programDriver.addClass("sortH", SortHeight.class,
                    "A map/reduce program that shows the height for each species of tree in Paris sorted from the smallest to the largest.");

            programDriver.addClass("maxH", MaxHeight.class,
                    "A map/reduce program that shows the max height for each species of tree in Paris.");

            programDriver.addClass("nbTrees", NbTreesBySpecies.class,
                    "A map/reduce program that counts the number of trees by species in Paris.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```

#### Result
```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar oldest input-trees/* OldestTree

20/11/11 14:44:18 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/11 14:44:18 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605102258363, maxDate=1605707058363, sequenceNumber=6461, masterKeyId=46 on ha-hdfs:efrei
20/11/11 14:44:18 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605102258363, maxDate=1605707058363, sequenceNumber=6461, masterKeyId=46)
20/11/11 14:44:18 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_3851
20/11/11 14:44:19 INFO input.FileInputFormat: Total input files to process : 1
20/11/11 14:44:19 INFO mapreduce.JobSubmitter: number of splits:1
20/11/11 14:44:19 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_3851
20/11/11 14:44:19 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605102258363, maxDate=1605707058363, sequenceNumber=6461, masterKeyId=46)]
20/11/11 14:44:19 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/11 14:44:19 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/11 14:44:20 INFO impl.YarnClientImpl: Submitted application application_1603290159664_3851
20/11/11 14:44:20 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_3851/
20/11/11 14:44:20 INFO mapreduce.Job: Running job: job_1603290159664_3851
20/11/11 14:44:30 INFO mapreduce.Job: Job job_1603290159664_3851 running in uber mode : false
20/11/11 14:44:30 INFO mapreduce.Job:  map 0% reduce 0%
20/11/11 14:44:39 INFO mapreduce.Job:  map 100% reduce 0%
20/11/11 14:44:44 INFO mapreduce.Job:  map 100% reduce 100%
20/11/11 14:44:44 INFO mapreduce.Job: Job job_1603290159664_3851 completed successfully
20/11/11 14:44:44 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=1364
                FILE: Number of bytes written=496321
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=7
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=19683
                Total time spent by all reduces in occupied slots (ms)=12696
                Total time spent by all map tasks (ms)=6561
                Total time spent by all reduce tasks (ms)=3174
                Total vcore-milliseconds taken by all map tasks=6561
                Total vcore-milliseconds taken by all reduce tasks=3174
                Total megabyte-milliseconds taken by all map tasks=10077696
                Total megabyte-milliseconds taken by all reduce tasks=6500352
        Map-Reduce Framework
                Map input records=98
                Map output records=97
                Map output bytes=1164
                Map output materialized bytes=1364
                Input split bytes=114
                Combine input records=0
                Combine output records=0
                Reduce input groups=1
                Reduce shuffle bytes=1364
                Reduce input records=97
                Reduce output records=1
                Spilled Records=194
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=219
                CPU time spent (ms)=3510
                Physical memory (bytes) snapshot=1450827776
                Virtual memory (bytes) snapshot=7268720640
                Total committed heap usage (bytes)=1553465344
                Peak Map Physical memory (bytes)=1155489792
                Peak Map Virtual memory (bytes)=3391950848
                Peak Reduce Physical memory (bytes)=295337984
                Peak Reduce Virtual memory (bytes)=3876769792
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=7


-sh-4.2$ hdfs dfs -cat /user/apignerol/OldestTree/part-r-00000
7       1601
```
##### There is also the pair (5, 1601) which exists, but we keep only the first one and do not print equal values



### 1.8.7 - Distric containing the most trees

#### SumTrees.java
```Java
package com.opstty.job;

import com.opstty.mapper.SumTreesMapper1;
import com.opstty.reducer.SumTreesReducer1;
import com.opstty.mapper.SumTreesMapper2;
import com.opstty.reducer.SumTreesReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SumTrees {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Path path = new Path("out/");

        if(otherArgs.length < 2)
        {
            System.err.println("Usage: sum <in> [<in>...] <out>");
            System.exit(2);
        }


        // 1st job : count how many trees each district contains
        Job job1 = Job.getInstance(conf, "sum");
        job1.setJarByClass(SumTrees.class);
        job1.setMapperClass(SumTreesMapper1.class);
        job1.setReducerClass(SumTreesReducer1.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);

        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job1, path);


        job1.waitForCompletion(true);

        // 2nd job : find the maximum number of trees

        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2);
        job2.setJarByClass(SumTrees.class);
        job2.setMapperClass(SumTreesMapper2.class);
        job2.setReducerClass(SumTreesReducer2.class);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(PairSumTrees.class);

        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, path);

        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
```

#### SumTreesMapper1.java
```Java
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
```

#### SumTreesReducer1.java
```Java
package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class SumTreesReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    // Give the count of trees of each district

    public void reduce(IntWritable district, Iterable<IntWritable> ids, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for(IntWritable val : ids) {
            sum++;
        }
        context.write(district, new IntWritable(sum));

    }
}
```



#### SumTreesMapper2.java
```Java
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
```


#### SumTreesReducer2.java
```Java
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
```


#### PairSumTrees.java
```Java
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
```


#### AppDriver.java
```Java
package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("sum", SumTrees.class,
                    "A map/reduce program that shows the district containing the most trees.");

            programDriver.addClass("oldest", OldestTree.class,
                    "A map/reduce program that shows the oldest tree and its district.");

            programDriver.addClass("sortH", SortHeight.class,
                    "A map/reduce program that shows the height for each species of tree in Paris sorted from the smallest to the largest.");

            programDriver.addClass("maxH", MaxHeight.class,
                    "A map/reduce program that shows the max height for each species of tree in Paris.");

            programDriver.addClass("nbTrees", NbTreesBySpecies.class,
                    "A map/reduce program that counts the number of trees by species in Paris.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```

#### Result
```CMD
-sh-4.2$ yarn jar /home/apignerol/hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar sum input-trees/* SumNbTrees

20/11/12 13:06:08 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/12 13:06:08 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605182768495, maxDate=1605787568495, sequenceNumber=7211, masterKeyId=47 on ha-hdfs:efrei
20/11/12 13:06:08 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605182768495, maxDate=1605787568495, sequenceNumber=7211, masterKeyId=47)
20/11/12 13:06:08 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_4484
20/11/12 13:06:09 INFO input.FileInputFormat: Total input files to process : 1
20/11/12 13:06:09 INFO mapreduce.JobSubmitter: number of splits:1
20/11/12 13:06:09 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_4484
20/11/12 13:06:09 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605182768495, maxDate=1605787568495, sequenceNumber=7211, masterKeyId=47)]
20/11/12 13:06:09 INFO conf.Configuration: found resource resource-types.xml at file:/etc/hadoop/3.1.5.0-152/0/resource-types.xml
20/11/12 13:06:09 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/12 13:06:10 INFO impl.YarnClientImpl: Submitted application application_1603290159664_4484
20/11/12 13:06:10 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_4484/
20/11/12 13:06:10 INFO mapreduce.Job: Running job: job_1603290159664_4484
20/11/12 13:06:20 INFO mapreduce.Job: Job job_1603290159664_4484 running in uber mode : false
20/11/12 13:06:20 INFO mapreduce.Job:  map 0% reduce 0%
20/11/12 13:06:30 INFO mapreduce.Job:  map 100% reduce 0%
20/11/12 13:06:35 INFO mapreduce.Job:  map 100% reduce 100%
20/11/12 13:06:35 INFO mapreduce.Job: Job job_1603290159664_4484 completed successfully
20/11/12 13:06:35 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=976
                FILE: Number of bytes written=495535
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=16892
                HDFS: Number of bytes written=80
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=20976
                Total time spent by all reduces in occupied slots (ms)=11380
                Total time spent by all map tasks (ms)=6992
                Total time spent by all reduce tasks (ms)=2845
                Total vcore-milliseconds taken by all map tasks=6992
                Total vcore-milliseconds taken by all reduce tasks=2845
                Total megabyte-milliseconds taken by all map tasks=10739712
                Total megabyte-milliseconds taken by all reduce tasks=5826560
        Map-Reduce Framework
                Map input records=98
                Map output records=97
                Map output bytes=776
                Map output materialized bytes=976
                Input split bytes=114
                Combine input records=0
                Combine output records=0
                Reduce input groups=17
                Reduce shuffle bytes=976
                Reduce input records=97
                Reduce output records=17
                Spilled Records=194
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=196
                CPU time spent (ms)=3170
                Physical memory (bytes) snapshot=1448599552
                Virtual memory (bytes) snapshot=7267921920
                Total committed heap usage (bytes)=1540882432
                Peak Map Physical memory (bytes)=1156435968
                Peak Map Virtual memory (bytes)=3394301952
                Peak Reduce Physical memory (bytes)=292163584
                Peak Reduce Virtual memory (bytes)=3873619968
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=16778
        File Output Format Counters
                Bytes Written=80
20/11/12 13:06:35 INFO client.AHSProxy: Connecting to Application History server at hadoop-master03.efrei.online/163.172.100.24:10200
20/11/12 13:06:35 INFO hdfs.DFSClient: Created token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605182795945, maxDate=1605787595945, sequenceNumber=7212, masterKeyId=47 on ha-hdfs:efrei
20/11/12 13:06:35 INFO security.TokenCache: Got dt for hdfs://efrei; Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605182795945, maxDate=1605787595945, sequenceNumber=7212, masterKeyId=47)
20/11/12 13:06:35 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
20/11/12 13:06:35 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /user/apignerol/.staging/job_1603290159664_4485
20/11/12 13:06:36 INFO input.FileInputFormat: Total input files to process : 1
20/11/12 13:06:36 INFO mapreduce.JobSubmitter: number of splits:1
20/11/12 13:06:36 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1603290159664_4485
20/11/12 13:06:36 INFO mapreduce.JobSubmitter: Executing with tokens: [Kind: HDFS_DELEGATION_TOKEN, Service: ha-hdfs:efrei, Ident: (token for apignerol: HDFS_DELEGATION_TOKEN owner=apignerol@EFREI.ONLINE, renewer=yarn, realUser=, issueDate=1605182795945, maxDate=1605787595945, sequenceNumber=7212, masterKeyId=47)]
20/11/12 13:06:36 INFO impl.TimelineClientImpl: Timeline service address: hadoop-master03.efrei.online:8190
20/11/12 13:06:37 INFO impl.YarnClientImpl: Submitted application application_1603290159664_4485
20/11/12 13:06:37 INFO mapreduce.Job: The url to track the job: https://hadoop-master01.efrei.online:8090/proxy/application_1603290159664_4485/
20/11/12 13:06:37 INFO mapreduce.Job: Running job: job_1603290159664_4485
20/11/12 13:06:47 INFO mapreduce.Job: Job job_1603290159664_4485 running in uber mode : false
20/11/12 13:06:47 INFO mapreduce.Job:  map 0% reduce 0%
20/11/12 13:06:56 INFO mapreduce.Job:  map 100% reduce 0%
20/11/12 13:07:07 INFO mapreduce.Job:  map 100% reduce 100%
20/11/12 13:07:07 INFO mapreduce.Job: Job job_1603290159664_4485 completed successfully
20/11/12 13:07:07 INFO mapreduce.Job: Counters: 53
        File System Counters
                FILE: Number of bytes read=244
                FILE: Number of bytes written=493827
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=189
                HDFS: Number of bytes written=6
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
        Job Counters
                Launched map tasks=1
                Launched reduce tasks=1
                Data-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=20442
                Total time spent by all reduces in occupied slots (ms)=28656
                Total time spent by all map tasks (ms)=6814
                Total time spent by all reduce tasks (ms)=7164
                Total vcore-milliseconds taken by all map tasks=6814
                Total vcore-milliseconds taken by all reduce tasks=7164
                Total megabyte-milliseconds taken by all map tasks=10466304
                Total megabyte-milliseconds taken by all reduce tasks=14671872
        Map-Reduce Framework
                Map input records=17
                Map output records=17
                Map output bytes=204
                Map output materialized bytes=244
                Input split bytes=109
                Combine input records=0
                Combine output records=0
                Reduce input groups=1
                Reduce shuffle bytes=244
                Reduce input records=17
                Reduce output records=1
                Spilled Records=34
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=172
                CPU time spent (ms)=3190
                Physical memory (bytes) snapshot=1453764608
                Virtual memory (bytes) snapshot=7275376640
                Total committed heap usage (bytes)=1549271040
                Peak Map Physical memory (bytes)=1153449984
                Peak Map Virtual memory (bytes)=3396513792
                Peak Reduce Physical memory (bytes)=300314624
                Peak Reduce Virtual memory (bytes)=3878862848
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters
                Bytes Read=80
        File Output Format Counters
                Bytes Written=6




-sh-4.2$ hdfs dfs -cat /user/apignerol/SumNbTrees/part-r-00000

16      36

```