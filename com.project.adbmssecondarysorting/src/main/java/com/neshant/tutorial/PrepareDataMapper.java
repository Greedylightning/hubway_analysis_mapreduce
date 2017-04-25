package com.neshant.tutorial;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrepareDataMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
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

		String time = tokens[1].replaceAll("\"", "");
		String timeOfTheDay = time.toString();
		DateFormat inFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		Date date = null;
		try {
			date = inFormat.parse(timeOfTheDay);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		int month = cal.get(Calendar.MONTH);

		String startStationID = tokens[3].replaceAll("\"", "");
		String EndStationID = tokens[7].replaceAll("\"", "");

		context.write(new Text(month + "," + startStationID + "," + EndStationID),NullWritable.get());

	}

}