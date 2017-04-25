package hubway_first;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

	public static TreeMap<Frequency, Text> ToRecordMap = new TreeMap<Frequency, Text>(new FrequencyComp());

	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		for (Text value : values) {

			String line = value.toString();

			if (line.length() > 0) {

				String[] tokens = line.split(",");

				// split the data and fetch salary

				int salary = Integer.parseInt(tokens[1]);

				// insert salary as key and entire row as value

				// tree map sort the records based on salary

				ToRecordMap.put(new Frequency(salary), new Text(value));

			}

		}

		// If we have more than ten records, remove the one with the lowest sal

		// As this tree map is sorted in descending order, the user with

		// the lowest sal is the last key.

		Iterator<Entry<Frequency, Text>> iter = ToRecordMap.entrySet().iterator();

		Entry<Frequency, Text> entry = null;

		while (ToRecordMap.size() > 10) {

			entry = iter.next();

			iter.remove();

		}

		for (Text t : ToRecordMap.descendingMap().values()) {

			// Output our ten records to the file system with a null key

			context.write(NullWritable.get(), t);

		}

	}

}