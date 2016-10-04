import java.io.IOException;
import java.util.Iterator;
 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  
 // Create a variable to store the total count
 private IntWritable totalCount = new IntWritable();
  
 @Override
 public void reduce(Text key, Iterable<IntWritable> values, Context context_W)
            throws IOException, InterruptedException {
				
  // Initialize wordCount to 0 for each word
  int wordCount = 0;
  
  //Iterate through the values  
  Iterator<IntWritable> it=values.iterator();
  while (it.hasNext()) {
   wordCount += it.next().get();
  }
  totalWordCount.set(wordCount);
  context_W.write(key, totalCount);
 }
}
