import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// The mapper class
public class WordMapper extends Mapper<LongWritable, Text, Text, TupleWritable>
{
  // The map method runs once for each line of text in the input file
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException
  {
    // Get the line as a simple Java String
    String line = value.toString();
    String[] data = line.split("\\t");

    String x = context.getConfiguration().get("month1");
    String y = context.getConfiguration().get("month2");
    
    String hashTagName = "";
    String month = "";
    int count = 1; 
   
    //for(int i = 0; i < data.size(); ++i)
    
       if(data[1].equalsIgnoreCase(x) || data[1].equalsIgnoreCase(y))
         {
            month = data[1];
            count = Integer.parseInt(data[2]);
            hashTagName = data[3]; 
            context.write(new Text(hashTagName), new TupleWritable(month, count));
  
         } 
    
  

    //context.write(new Text(hashTagName), new TupleWritable(month, count));
  


    

  }
}
