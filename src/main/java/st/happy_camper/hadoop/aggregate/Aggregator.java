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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author ueshin
 */
public class Aggregator extends Configured implements Tool {

    /**
     * @author ueshin
     */
    private static class Map extends
            Mapper<LongWritable, Text, AccessWritable, IntWritable> {

        private static final Pattern PATTERN = Pattern
                .compile("^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) .* "
                        + "\\[(\\d{2}/[A-Z][a-z][a-z]/\\d{4}):\\d{2}:\\d{2}:\\d{2} [-+]\\d{4}\\] "
                        + "\"GET ((?:/[^ ]*)?/(?:[^/]+\\.html)?) HTTP/1\\.[01]\" (?:200|304) .*$");

        private final AccessWritable access = new AccessWritable();

        private final IntWritable one = new IntWritable(1);

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
         * org.apache.hadoop.mapreduce.Mapper.Context)
         */
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            Matcher matcher = PATTERN.matcher(line);
            if(matcher.matches()) {
                try {
                    String ip = matcher.group(1);
                    String url = matcher.group(3);
                    if(url.endsWith("/")) {
                        url = url + "index.html";
                    }
                    Date accessDate = new SimpleDateFormat("dd/MMM/yyyy",
                            Locale.US).parse(matcher.group(2));
                    access.setAccess(new Access(ip, url, accessDate));
                }
                catch(ParseException e) {
                    e.printStackTrace();
                    return;
                }
                context.write(access, one);
            }
        }
    }

    /**
     * @author ueshin
     */
    private static class Combine extends
            Reducer<AccessWritable, IntWritable, AccessWritable, IntWritable> {

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN,
         * java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
         */
        @Override
        public void reduce(AccessWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }

    }

    /**
     * @author ueshin
     */
    private static class Reduce extends
            Reducer<AccessWritable, IntWritable, Text, IntWritable> {

        /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
         * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
         * org.apache.hadoop.mapred.Reporter)
         */
        @Override
        public void reduce(AccessWritable key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable value : values) {
                sum += value.get();
            }
            context.write(new Text(String.format("%s\t%s\t%s", key.getAccess()
                    .getIp(), key.getAccess().getUrl(), new SimpleDateFormat(
                    "yyyy/MM/dd").format(key.getAccess().getAccessDate()))),
                    new IntWritable(sum));
        }
    }

    /*
     * (non-Javadoc)
     * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
     */
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf(), "aggregator");

        job.setJarByClass(getClass());

        FileInputFormat.setInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(AccessWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);

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
