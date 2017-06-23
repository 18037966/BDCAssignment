import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

// The driver class
public class WordCount
{
  public static void main(String[] args) throws Exception
  {
    if(args.length == 4)
    {
      // Instantiate a Job object for your job's configuration
      Configuration conf = new Configuration();
      conf.set("month1", args[2]);
      conf.set("month2", args[3]);

      Job job = new Job(conf);
      
      // Tell Hadoop which Jar file contains our code
      job.setJarByClass(WordCount.class);
      
      // Set a job name which will appear in logs
      job.setJobName("Word Count");

      // Set input and output names from arguments
      FileInputFormat.setInputPaths(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      // Set mapper and reducer classes
      job.setMapperClass(WordMapper.class);
      job.setReducerClass(SumReducer.class);

      // Set key and value types for the mapper output
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(TupleWritable.class);

      // Set key and value types for the reducer (final) output
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      // Start the MapReduce job and wait for it to finish
      boolean success = job.waitForCompletion(true);
      System.out.printf("Job %s.", success ? "succeeded" : "failed");
    }
    else
    {
      System.out.println("Usage: WordCount <input dir> <output dir>");
    }
  }
}
