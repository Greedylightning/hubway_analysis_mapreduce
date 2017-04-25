package hubway_first;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	// create the Tree Map with MySalaryComparator

	public static TreeMap<Frequency, Text> ToRecordMap = new TreeMap<Frequency, Text>(new FrequencyComp());

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();

		String[] tokens = line.split(",");

		// split the data and fetch salary

		int salary = Integer.parseInt(tokens[1]);

		// insert salary object as key and entire row as value

		// tree map sort the records based on salary

		ToRecordMap.put(new Frequency(salary), new Text(value));

		// If we have more than ten records, remove the one with the lowest
		// salary

		// As this tree map is sorted in descending order, the employee with

		// the lowest salary is the last key.

		Iterator<Entry<Frequency, Text>> iter = ToRecordMap.entrySet().iterator();

		Entry<Frequency, Text> entry = null;

		while (ToRecordMap.size() > 10) {

			entry = iter.next();

			iter.remove();

		}

	}

	protected void cleanup(Context context) throws IOException, InterruptedException {

		// Output our ten records to the reducers with a null key

		for (Text t : ToRecordMap.values()) {

			context.write(NullWritable.get(), t);

		}

	}

}