package hubway_first;

import java.io.IOException;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, SortedMapWritable, Text, MedianSDCustomWritable> {

	private MedianSDCustomWritable result = new MedianSDCustomWritable();
	private TreeMap<Float, Long> rating_count_map = new TreeMap<Float, Long>();

	public void reduce(Text key, Iterable<SortedMapWritable> values, Context context)
			throws IOException, InterruptedException {

		float sum_of_product_of_raiting_count = 0;
		long totalRatings = 0;
		rating_count_map.clear();
		result.setMedian(0);
		result.setStandardDeviation(0);
		result.setAverage(0);

		result.setStandardDeviation(0);

		for (SortedMapWritable v : values) {
			for (Entry<WritableComparable, Writable> entry : v.entrySet()) {
				Float rating = ((FloatWritable) entry.getKey()).get();
				long count_of_each_rating = ((LongWritable) entry.getValue()).get();

				totalRatings += count_of_each_rating;
				sum_of_product_of_raiting_count += rating * count_of_each_rating;

				Long storedCount = rating_count_map.get(rating);
				if (storedCount == null) {
					rating_count_map.put(rating, count_of_each_rating);
				} else {
					rating_count_map.put(rating, storedCount + count_of_each_rating);
				}
			}
		}

		long medianIndex = totalRatings / 2L;
		long previousRatings = 0;
		long rating = 0;
		float prevKey = 0;

		for (Entry<Float, Long> entry : rating_count_map.entrySet()) {
			rating = previousRatings + entry.getValue();
			if (previousRatings <= medianIndex && medianIndex < rating) {
				if (totalRatings % 2 == 0 && previousRatings == medianIndex) {
					result.setMedian((entry.getKey() + prevKey) / 2);
				} else {
					result.setMedian(entry.getKey());
				}
				break;
			}
			previousRatings = rating;
			prevKey = entry.getKey();
		}

		// calculate standard deviation
		double mean = sum_of_product_of_raiting_count / totalRatings;
		result.setAverage(mean);

		double sumOfSquares = 0;
		for (Entry<Float, Long> entry : rating_count_map.entrySet()) {
			sumOfSquares += (entry.getKey() - mean) * (entry.getKey() - mean) * entry.getValue();
		}

		result.setStandardDeviation((double) Math.sqrt(sumOfSquares / (totalRatings - 1)));

		context.write(key, result);

	}
}