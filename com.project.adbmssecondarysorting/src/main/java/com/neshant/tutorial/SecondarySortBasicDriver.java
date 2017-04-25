package com.neshant.tutorial;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortBasicDriver extends Configured implements Tool {

	public int run(String[] args) throws Exception {

		if (args.length != 3) {
			System.out.printf("Two parameters are required for SecondarySortBasicDriver- <input dir> <output dir>\n");
			return -1;
		}

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);
		Path second_input_path = outputDir;
		Path final_output = new Path(args[2]);

		// Create configuration
		Configuration conf = new Configuration(true);

		// Create job
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(PrepareDataMapper.class);

		// Setup MapReduce
		job.setMapperClass(PrepareDataMapper.class);
		job.setReducerClass(PrepareDataReducer.class);
		job.setNumReduceTasks(1);

		// Specify key / value
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// Input
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(job, outputDir);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		boolean complete = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "chaining");

		if (complete) {
			job2.setJarByClass(SecondarySortBasicDriver.class);
			
			job2.setMapperClass(SecondarySortBasicMapper.class);
			job2.setMapOutputKeyClass(CompositeKeyWritable.class);
			job2.setMapOutputValueClass(NullWritable.class);
			
			job2.setPartitionerClass(SecondarySortBasicPartitioner.class);
			job2.setSortComparatorClass(SecondarySortBasicCompKeySortComparator.class);
			job2.setGroupingComparatorClass(SecondarySortBasicGroupingComparator.class);
			
			job2.setReducerClass(SecondarySortBasicReducer.class);
			job2.setOutputKeyClass(CompositeKeyWritable.class);
			job2.setOutputValueClass(NullWritable.class);
			
			job2.setNumReduceTasks(12);

			// Delete output if exists
			FileSystem hdfs2 = FileSystem.get(conf2);
			if (hdfs2.exists(final_output))
				hdfs2.delete(final_output, true);

			
			FileInputFormat.addInputPath(job2, second_input_path);
			FileOutputFormat.setOutputPath(job2, final_output);
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}

		boolean success = job2.waitForCompletion(true);
		return success ? 0 : 1;

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SecondarySortBasicDriver(), args);
		System.exit(exitCode);
	}
}