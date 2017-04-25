package hubway_first;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusiestDistincMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	// private static int year1 = Calendar.getInstance().get(Calendar.YEAR);

	// private Text gender = new Text();
	// private int birthYear;
	// private IntWritable age = new IntWritable();
	//

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (key.get() == 0) {
			return;
		}
		String[] tokens = value.toString().split(",");
		String startStationID = tokens[4].replaceAll("\"", "");
		// String endStationId = tokens[7].replaceAll("\"", "");

		context.write(new Text(startStationID), NullWritable.get());

	}

}