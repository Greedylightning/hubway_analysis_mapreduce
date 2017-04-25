package hubway_first;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusiestDistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	public void reduce(Text text, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		
		for (NullWritable time : values) {
			
		}
		context.write(text, NullWritable.get());

	}
}