package hubway_first;

import java.io.IOException;

import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageAgeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	public void reduce(Text text, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		
		int sumOfAge = 0;
		int count = 0;
		for (IntWritable age : values) {
			if (age.equals(null)){continue;}
			count++;
			sumOfAge += age.get();
			
		}

		//IntWritable averageAge;	averageAge.set(sumOfAge / count);

		context.write(text, new IntWritable(sumOfAge / count));

	}
}