package com.neshant.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrepareDataReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

	public void reduce(Text text, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {

		int count = 0;

		for (NullWritable time : values) {
			count++;
		}

		context.write(new Text(text + "," + count),NullWritable.get());

	}
}