package hubway_first;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrafficByHourReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text text, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		
		int sumOfAge = 0;
		int count = 0;
		int hour = 0;
		for (Text time : values) {
			if (time.equals(null)){continue;}
			count++;
			
			
			
			String timeOfTheDay = time.toString();
			DateFormat inFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

			Date date = null;
			try {
				date = inFormat.parse(timeOfTheDay);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			 hour = date.getHours();
			
		}

		//IntWritable averageAge;	averageAge.set(sumOfAge / count);

		context.write(text, new Text(hour+","+count));

	}
}