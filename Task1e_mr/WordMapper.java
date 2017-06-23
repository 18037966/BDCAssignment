import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The mapper class
public class WordMapper extends Mapper<LongWritable, Text, TupleWritable, Text>
{
  // The map method runs once for each line of text in the input file
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
  {
    // Get the line as a simple Java String
    String line = value.toString();
    String[] data = line.split(";");

    //ArrayList<String> data = new ArrayList<String>(); 
    //for (String word : line.split(";"))
    //{
      //data.add(word);     
      //context.write(new Text(job[1]), new IntWritable(1));  
    //}

    //String key = data.get(1) + ", " + data.get(2) + ", " + data.get(7);

    String edu = data[3];
    int    bal = Integer.parseInt(data[5]);
    String xxx = data[1] + ", " + data[2] + ", " + data[7];
    //TupleWritable tuple = new TupleWritable(key, value);     
    context.write(new TupleWritable(edu, bal), new Text(xxx));

  }
}
