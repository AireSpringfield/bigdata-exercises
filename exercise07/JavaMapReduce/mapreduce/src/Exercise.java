import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Exercise {
    
    private static class WordGroupingMapper extends Mapper<Object, Text, Text, IntWritable>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                bool bEmit = false;
                String wordStr = itr.nextToken();
                if(wordStr.charAt(0) == 'H') // Question 1: number of distinct words begin with H
                {
                    bEmit = true;
                }

                if(bEmit){
                    word.set(wordStr);
                    context.write(word, one);
                }
            }
        }
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
    }

    // Question 2: number of words (not distinct) end with g
    private static class Mapper2 extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }


    // Question 3: number of words (not distinct) of length 12
    private static class Mapper3 extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private static class WordGroupingReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        public static bool bDistinct = false;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            if(!bDistinct)
            {
                for(IntWritable v : values)
                {
                    resultVal += v.get(); 
                }
                result.set(resultVal);
                context.write(key, new IntWritable(resultVal));
            }
            else
            {
                context.write(key, new IntWritable(1));
            }


        }
    }


    private static class CountMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
        public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value); // Just an identity
        }
    }
    private static class CountReducer extends Reducer<Text,IntWritable,Object,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int resultVal = 0;
            for(IntWritable v : values)
            {
                resultVal += v.get(); 
            }
            result.set(resultVal);
            context.write(new Object(), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: 2 required params <hdfs_file_in> <hdfs_file_out>");
            System.exit(2);
        }

        // First, group words
        bool bFirstJobSuccess = false;
        {
            Job job = new Job(conf, "WordGrouping");
            job.setJarByClass(Exercise.class);
            job.setMapperClass(WordGroupingMapper.class);
            WordGroupingReducer.bDistinct = true;
            job.setReducerClass(WordGroupingReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]) + "/intermediate");
            
            bFirstJobSuccess = job.waitForCompletion(true);
        }

        // Then, count the frequency of all words
        if(bFirstJobSuccess)
        {
            Job job = new Job(conf, "WordCounting");
            job.setJarByClass(Exercise.class);
            job.setMapperClass(CountMapper.class);
            job.setReducerClass(CountReducer.class);
            job.setOutputKeyClass(Object.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[1]) + "/intermediate"));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]) + "/result");

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }

}
