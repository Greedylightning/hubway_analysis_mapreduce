package com.neshant.tutorial;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortBasicMapper extends Mapper<LongWritable, Text, CompositeKeyWritable, NullWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		if (value.toString().length() > 0) {

			String values[] = value.toString().split(",");

			if (key.get() == 0)
				return;
			else {

				// IntWritable month = new
				// IntWritable(Integer.parseInt(values[0].toString()));
				int month = Integer.parseInt(values[0].toString());
				int count = Integer.parseInt(values[3].toString());
				String route = values[1].toString() + "," + values[2].toString();

				// IntWritable count = new
				// IntWritable(Integer.parseInt(values[3].toString()));
				CompositeKeyWritable cw = new CompositeKeyWritable(month, count, route);
				try {
					context.write(cw, NullWritable.get());

				} catch (Exception e) {
					System.out.println("" + e.getMessage());
				}
			}

		}

	}
}