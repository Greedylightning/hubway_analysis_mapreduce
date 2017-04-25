package hubway_first;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AveragePartitioner extends Partitioner<Text, SortedMapWritable> {

	@Override
	public int getPartition(Text key, SortedMapWritable value, int numReduceTasks) {

		String[] tokens = key.toString().split("_");
		return Integer.parseInt(tokens[1].trim()) % numReduceTasks;
		//
		// return (key.getMonth() % numReduceTasks);
	}
}