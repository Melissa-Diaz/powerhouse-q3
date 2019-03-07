package edu.utdallas.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;

public class Question3 {

    public static class BusinessMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String [] business = line.split("::");

            String addrCat = "Business Info:: " + business[1] + "::" + business[2] ;

            context.write(new Text(business[0]), new Text(addrCat));
        }
    }

    public static class ReviewsMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            String [] review = line.split("::");

            context.write(new Text(review[2]), new Text("Review:: " + review[3]));
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Double reviewSum = 0.0;
            Double avgRating = 0.0;
            int reviewCount = 0;
            boolean bizFound = false;
            String [] reviewInfo = null;
            String [] bizInfo = null;
            String bizKey = "";
            String avgRatingString = "";

            for (Text t : values)
            {
               if (t.toString().contains ("Review")) {
                   reviewInfo = t.toString().split("::");
                   reviewSum += Float.parseFloat(reviewInfo[1]);
                   reviewCount++;
               } else if (t.toString().contains("Business Info") && bizFound == false){
                   bizInfo = t.toString().split("::");
                   bizFound = true;
                   bizKey  = key + "\t" + bizInfo[1] + "\t" + bizInfo[2];
               }
            }

            avgRating= reviewSum/reviewCount;
            avgRatingString = Double.toString(avgRating);
            context.write(new Text(bizKey), new Text(avgRatingString));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        FileSystem fs = null;
        boolean status = false;

        Configuration conf = new Configuration();
        /* set this up so I could run in IDE and now have to jar it up and run hadoop <jar> blah blahA */

        /*
        conf.addResource(new Path("/usr/local/Cellar/hadoop/3.1.1/libexec/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/Cellar/hadoop/3.1.1/libexec/etc/hadoop/mapred-site.xml"));
        */


        /* do not feel like removing the output file every time this is run */
        fs = FileSystem.get(conf);
        Path outputPath = new Path(args[2]);
        if (fs.exists(outputPath))
          status = fs.delete(new Path(args[2]), true);

        Job job = Job.getInstance(conf,"Business_Reviews");
        job.setJarByClass(Question3.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
//        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);

        // set the HDFS path of the input data
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, BusinessMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReviewsMapper.class);
        job.setReducerClass(JoinReducer.class);

        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }
}
