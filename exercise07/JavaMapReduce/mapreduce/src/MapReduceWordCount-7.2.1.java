import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;

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

public class MapReduceWordCount {

    private static class WordCountMapper extends Mapper<Object, Text, IntWritable, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokens = new StringTokenizer(value.toString());
            
            while (tokens.hasMoreTokens()){
                String str = tokens.nextToken();
                if (str.charAt(0) == 'H'){
                    // distinct!
                    word.set(str);
                    context.write(one, word);
                }

            }
        }
    }

    private static class WordCountReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashSet<Text> hwords = new HashSet<Text>();

            for (Text value: values) {
                hwords.add(value);
            }
            
            int count = hwords.size();

            result.set(count);
            context.write(key, result);//
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: 2 required params <hdfs_file_in> <hdfs_file_out>");
            System.exit(2);
        }
        // Setup a MapReduce job
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduceWordCount.class);
        job.setMapperClass(WordCountMapper.class);
        
	// Can you reducer be used as a combiner? If so, enable it.
	// You can also experiment with and without combiner and compare the running time.
	    // job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
	// at least one reducer, experiment with this parameter
        job.setNumReduceTasks(8);    
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
