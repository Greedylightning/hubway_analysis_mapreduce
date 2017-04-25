package hubway_first;

import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author pragya mishra
 */
public class Lab_Join extends Configured implements Tool {

	public static class JoinMapper1 extends Mapper<Object, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] separatedInput = value.toString().split(",");

			String startingStationID = separatedInput[4].trim().replaceAll("\"", "");

			if (startingStationID == null) {
				return;
			}
			outKey.set(startingStationID);
			outValue.set("A" + value);

			context.write(outKey, outValue);

		}

	}

	public static class JoinMapper2 extends Mapper<Object, Text, Text, Text> {

		private Text outKey = new Text();
		private Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String startingStationID = value.toString().split(",")[0].trim();
			if (startingStationID == null) {
				return;
			}
			outKey.set(startingStationID);
			outValue.set("B" + value);
			context.write(outKey, outValue);

		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

		private static final Text EMPTY_TEXT = new Text();
		private Text tmp = new Text();

		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();

		private String joinType = null;

		public void setup(Context context) {
			joinType = context.getConfiguration().get("join.type");
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			listA.clear();
			listB.clear();
			while (values.iterator().hasNext()) {
				tmp = values.iterator().next();

				if (tmp.charAt(0) == 'A') {
					listA.add(new Text(tmp.toString().substring(1)));
				} else if (tmp.charAt(0) == 'B') {
					listB.add(new Text(tmp.toString().substring(1)));
				}
			}

			executeJoinLogic(context);
		}

		private void executeJoinLogic(Context context) throws IOException, InterruptedException {
			if (joinType.equalsIgnoreCase("leftOuter")) {
				for (Text A : listA) {
					if (!listB.isEmpty()) {
						for (Text B : listB) {

							context.write(A, B);

							// Logger.getLogger(Lab_Join.class.getName()).log(Level.SEVERE,
							// null, ex);

						}
					} else {

						context.write(A, EMPTY_TEXT);

					}
				}
			} else if (joinType.equalsIgnoreCase("rightOuter")) {
				for (Text B : listB) {
					if (!listA.isEmpty()) {
						for (Text A : listA) {

							context.write(A, B);

						}
					} else {

						context.write(EMPTY_TEXT, B);

					}
				}
			} else if (joinType.equalsIgnoreCase("fullouter")) {
				if (!listA.isEmpty()) {
					for (Text A : listA) {
						if (!listB.isEmpty()) {
							for (Text B : listB) {

								context.write(A, B);

							}
						} else {

							context.write(A, EMPTY_TEXT);

						}

					}
				} else {
					for (Text B : listB) {

						context.write(EMPTY_TEXT, B);

					}
				}

			} else if (joinType.equalsIgnoreCase("anti")) {
				if (listA.isEmpty() ^ listB.isEmpty()) {
					for (Text A : listA) {

						context.write(A, EMPTY_TEXT);

					}
					for (Text B : listB) {

						context.write(EMPTY_TEXT, B);

					}
				}
			}
		}

	}

	public static void main(String[] args) {
		try {
			int res = ToolRunner.run(new Configuration(), new Lab_Join(), args);
		} catch (Exception ex) {
			Logger.getLogger(Lab_Join.class.getName()).log(Level.SEVERE, null, ex);

		}
	}

	public int run(String[] strings) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ReduceJoin");
		job.setJarByClass(Lab_Join.class);

		Path outputDir = new Path(strings[2]);

		MultipleInputs.addInputPath(job, new Path(strings[0]), TextInputFormat.class, JoinMapper1.class);
		MultipleInputs.addInputPath(job, new Path(strings[1]), TextInputFormat.class, JoinMapper2.class);
		job.getConfiguration().set("join.type", "leftouter");

		job.setReducerClass(JoinReducer.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, outputDir);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Delete output if exists
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);

		
		
		
		
		boolean success = job.waitForCompletion(true);
		return success ? 0 : 2;
	}
}