/*
 * Copyright 2008 Happy-Camper Street.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package st.happy_camper.hadoop.aggregate;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author ueshin
 */
public class Aggregator extends Configured implements Tool {

    /**
     * @author ueshin
     */
    private static class Map extends MapReduceBase implements
            Mapper<LongWritable, Text, AccessWritable, IntWritable> {

        private static final Pattern PATTERN = Pattern
                .compile("^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) .*\"GET (/[^ ]*) .*$");

        private static final AccessWritable access = new AccessWritable();

        private static final IntWritable one = new IntWritable(1);

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
         * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
         * org.apache.hadoop.mapred.Reporter)
         */
        @Override
        public void map(LongWritable key, Text value,
                OutputCollector<AccessWritable, IntWritable> output,
                Reporter reporter) throws IOException {
            String line = value.toString();
            Matcher matcher = PATTERN.matcher(line);
            if(matcher.matches()) {
                access
                        .setAccess(new Access(matcher.group(1), matcher
                                .group(2)));
                output.collect(access, one);
            }
        }
    }

    /**
     * @author ueshin
     */
    private static class Combine extends MapReduceBase implements
            Reducer<AccessWritable, IntWritable, AccessWritable, IntWritable> {

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
         * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
         * org.apache.hadoop.mapred.Reporter)
         */
        @Override
        public void reduce(AccessWritable key, Iterator<IntWritable> values,
                OutputCollector<AccessWritable, IntWritable> output,
                Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }

    }

    /**
     * @author ueshin
     */
    private static class Reduce extends MapReduceBase implements
            Reducer<AccessWritable, IntWritable, Text, IntWritable> {

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
         * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
         * org.apache.hadoop.mapred.Reporter)
         */
        @Override
        public void reduce(AccessWritable key, Iterator<IntWritable> values,
                OutputCollector<Text, IntWritable> output, Reporter reporter)
                throws IOException {
            int sum = 0;
            while(values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(new Text(String.format("%s\t%s", key.getAccess()
                    .getIp(), key.getAccess().getUrl())), new IntWritable(sum));
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName("aggregator");

        FileInputFormat.setInputPaths(conf, args[0]);
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combine.class);
        conf.setReducerClass(Reduce.class);

        conf.setOutputKeyClass(AccessWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        JobClient.runJob(conf);
        return 0;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Aggregator(), args));
    }

}
