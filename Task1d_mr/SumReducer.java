import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// The reducer class
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
  // The reduce method runs once for each key received after the shuffle phase
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException
  {
    int wordCount = 0;
    
    // Iterate over the values passed to us by the mapper
    for (IntWritable value : values)
    {
      // Add the value to the word count counter for this key.
      wordCount += value.get();
    }
    
    // Emit the word accompanied by its final count
    context.write(key, new IntWritable(wordCount));
  }
}
