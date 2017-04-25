package hubway_first;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TrafficByHourPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {

		//String[] lineRecord = value.toString().split(",");
		String timeOfTheDay = value.toString();
		DateFormat inFormat = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");

		Date date = null;
		try {
			date = inFormat.parse(timeOfTheDay);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int hour = date.getHours();
		// 2015-01-01 01:07:06
		// int ageInt = Integer.parseInt(age);
		return hour;

		

	}

}