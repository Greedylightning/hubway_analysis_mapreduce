package hubway_first;

import java.io.IOException;
import java.time.Year;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficByHourMapper extends Mapper<LongWritable, Text, Text, Text> {
	//private static  int year1 = Calendar.getInstance().get(Calendar.YEAR);

	//private Text gender = new Text();
	//private int birthYear;
	//private IntWritable age = new IntWritable();
//

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {



		if (key.get() == 0) {
			return;
		}
		String[] tokens = value.toString().split(",");
		String startStationID= tokens[3].replaceAll("\"", "");
String startTime = tokens[1].replaceAll("\"", "");
	

		context.write(new Text(startStationID), new Text(startTime));

	}

}