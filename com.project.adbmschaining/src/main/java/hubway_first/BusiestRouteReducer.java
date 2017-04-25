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

public class BusiestRouteReducer extends Reducer<Text, Text, NullWritable, Text> {

	public void reduce(Text text, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		for (Text time : values) {
			count++;

		}
		context.write(NullWritable.get(), new Text(text+","+count));

	}
}