import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The mapper class
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
  // The map method runs once for each line of text in the input file
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
  {
    // Get the line as a simple Java String
    String line = value.toString();
    ArrayList<String> job = new ArrayList<String>(); 
    //ArrayList<String> splitWord = new ArrayList<String>();
    
    // Split the line into words
    for (String word : line.split(";"))
    {
      job.add(word);     
      //context.write(new Text(job[1]), new IntWritable(1));
      
    }    

     int balance = Integer.parseInt(job.get(5));
    if(balance <= 500)
    {
      context.write(new Text("Low"), new IntWritable(1));
    }
    else if(balance >= 501 && balance <= 1500)
    {
      context.write(new Text("Medium"), new IntWritable(1));
    }
    else if(balance >= 1501)
    {
      context.write(new Text("High"), new IntWritable(1));
    }

  }
}
