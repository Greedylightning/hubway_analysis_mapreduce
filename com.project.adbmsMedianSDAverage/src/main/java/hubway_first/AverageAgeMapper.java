package hubway_first;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageAgeMapper extends Mapper<LongWritable, Text, Text, SortedMapWritable> {
	private static int year1 = Calendar.getInstance().get(Calendar.YEAR);
	private Text gender = new Text();
	private int birthYear;
	private FloatWritable age = new FloatWritable();
	private static final LongWritable ONE = new LongWritable(1);

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		SortedMapWritable rating_count = new SortedMapWritable();

		if (key.get() == 0) {
			return;
		}
		String[] tokens = value.toString().split(",");
		String genderFromFile = tokens[14].replaceAll("\"", "");

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
		String year = String.valueOf(cal.get(Calendar.YEAR));

		// if (!tokens[13].isEmpty()) {
		try {
			birthYear = Integer.parseInt(tokens[13].replaceAll("\"", ""));

		} catch (Exception e) {
			System.out.println("not number");
		}

		// }

		age.set(year1 - birthYear);
		if (genderFromFile.equals("1")) {
			gender.set("Male" + "_" + year);

		} else if (genderFromFile.equals("2")) {
			gender.set("Female" + "_" + year);
		} else {
			gender.set("Gender Not Specified" + "_" + year);
		}

		rating_count.put(age, ONE);
		context.write(gender, rating_count);

		// context.write(gender, age);

	}

}