package hubway_first;

import java.io.BufferedReader;

import java.io.IOException;

import java.io.InputStreamReader;

import java.net.URI;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.Mapper.Context;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

public class Sentiment_Analysis extends Configured implements Tool {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private URI[] files;

		private HashMap<String, String> AFINN_map = new HashMap<String, String>();

		@SuppressWarnings("deprecation")
		@Override

		public void setup(Context context) throws IOException

		{
			files = DistributedCache.getCacheFiles(context.getConfiguration());

			System.out.println("files:" + files);

			Path path = new Path(files[0]);

			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream in = fs.open(path);

			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			String line = "";

			while ((line = br.readLine()) != null)

			{

				String splits[] = line.split("\t");

				AFINN_map.put(splits[0], splits[1]);

			}

			br.close();

			in.close();

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String name;
			String twt;

			String tuple[] = value.toString().split("\\|");

			// JSONParser jsonParser = new JSONParser();

			if (tuple.length!=0) {

				String tweet_text = tuple[0].toString().trim();
				twt = tuple[0].toString().trim();
				String[] splits = twt.toString().split(" ");

				int sentiment_sum = 0;

				for (String word : splits) {

					if (AFINN_map.containsKey(word))

					{

						Integer x = new Integer(AFINN_map.get(word));

						sentiment_sum += x;

					}

				}

				context.write(new Text("sentiment"),
						new Text(Randomiser.randBetween1(2015, 2016) + "\t----->\t" + new Text(Integer.toString(sentiment_sum))));

			}

		}

	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// public void reduce(Text text, Iterable<NullWritable> values,
			// Context context)

			for (Text time : values) {

				context.write(key, time);
			}

		}

	}

	public static void main(String[] args) throws Exception

	{

		ToolRunner.run(new Sentiment_Analysis(), args);

	}

	public int run(String[] args) throws Exception {

		// TODO Auto-generated method stub

		Configuration conf = new Configuration();

		// if (args.length != 2) {
		//
		// System.err.println("Usage: Parse <in> <out>");
		//
		// System.exit(2);
		//
		// }

		// DistributedCache.addCacheFile(new URI("/AFINN.txt"), conf);
		Path final_output = new Path(args[1]);
		Job job = new Job(conf, "SentimentAnalysis");

		job.setJarByClass(Sentiment_Analysis.class);

		job.setMapperClass(Map.class);

		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);

		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, final_output);
		DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(final_output))
			hdfs.delete(final_output, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

		return 0;

	}

}