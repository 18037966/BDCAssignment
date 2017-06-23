import java.io.IOException;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// The reducer class
public class SumReducer extends Reducer<Text, TupleWritable, Text, Text>
{
  // variables that contain highest tuple/count so far.
  String highestTag = "";
  int highestIncrease = 0;
  String xMon = "";
  int    x    = 0;
  String yMon = "";
  int    y    = 0;
  //String c = "";

  // The reduce method runs once for each key received after the shuffle phase
  @Override
  public void reduce(Text key, Iterable<TupleWritable> values, Context context)
      throws IOException, InterruptedException
  {
    String xMonName = context.getConfiguration().get("month1");
    String yMonName = context.getConfiguration().get("month2");

    String m0 = "";
    int    c0 = 0;
    String m1 = "";
    int    c1 = 0;


    for (TupleWritable value : values)
    {
      if (m0.equals(""))
      {
        m0 = value.month;
        c0 = value.count;
      }
      else
      {
        m1 = value.month;
        c1 = value.count; 
      }
    }

    // Handle zero cases.
    if (m1.equals(""))
    {
      if (m0.equals(yMonName))
      {
        m1 = xMonName;
      }
      else
      {
        m1 = yMonName;
      }
    }

    /*
    
      ArrayList<Integer> tupleCount = new ArrayList<Integer>();
      

      // get the first month
      // get the second month
      // compute increase
      // compare with current highest and replace highest if this is higher.
    for (TupleWritable value : values)
    {
       tupleCount.add(value.count);
              
    }
    */

    int increase;
    if(m0.equals(xMonName))
    {
      increase = c1 - c0; 
    }
    else
    {
      increase = c0 - c1;
    }

    
    if(increase > highestIncrease)
    {
       highestTag      = key.toString();
       highestIncrease = increase;
       xMon = m0;
       x    = c0;
       yMon = m1;
       y    = c1;
    }
  }

  // write cleanup method
  //context.write(hightest, new Text(highest));
   protected void cleanup(Context context) throws IOException, InterruptedException
   {
         System.out.println(highestTag);
         System.out.println(x);
         System.out.println(y);
         String c =  Integer.toString(x) + ", " + Integer.toString(y); 
         context.write(new Text(highestTag), new Text(c));

   }
}
