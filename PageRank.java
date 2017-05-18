package org.myorg;

/*
 * Name - Satish Kumar
 * 
 * Email ID - skumar34@uncc.edu
 * 
 * 
 * */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

	private static final String N_VALUE = "nvalue";
	
	static List<String> titleList = new ArrayList<String>();

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int res = ToolRunner.run(new PageRank(), args);
		System.exit(res);

	}

	// Setting job to run the Map and Reduce task
	public int run(String[] args) throws Exception {

		FileSystem fileSystem = FileSystem.get(getConf());
		Job jobCounter = Job.getInstance(getConf(), "pagerank");
		jobCounter.setJarByClass(this.getClass());
		long nVal;

		// jobCounter to calculate the value of N (total number of titles)
		FileInputFormat.addInputPaths(jobCounter, args[0]);
		FileOutputFormat.setOutputPath(jobCounter, new Path(
				"/home/cloudera/tempOut"));
		jobCounter.setMapperClass(MapCounter.class);
		jobCounter.setReducerClass(ReduceCounter.class);
		jobCounter.setMapOutputValueClass(IntWritable.class);
		jobCounter.setOutputKeyClass(Text.class);
		jobCounter.setOutputValueClass(IntWritable.class);
		jobCounter.waitForCompletion(true);
		fileSystem.delete(new Path("/home/cloudera/tempOut"), true);

		// calculated N value
		nVal = jobCounter.getCounters().findCounter("TotalPage", "TotalPage")
				.getValue();

		// Job to get the title and link graph
		Job jobLink = Job.getInstance(getConf(), "pagerank");
		jobLink.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(jobLink, args[0]);
		FileOutputFormat.setOutputPath(jobLink, new Path(
				"/home/cloudera/tempOut1" + "job0"));
		jobLink.getConfiguration().setStrings(N_VALUE, nVal + "");
		jobLink.setMapperClass(MapLink.class);
		jobLink.setReducerClass(ReduceLink.class);
		jobLink.setOutputKeyClass(Text.class);
		jobLink.setOutputValueClass(Text.class);
		jobLink.waitForCompletion(true);

		// Job to calculate the final page rank by iterating for 10 iterations

		int i = 1;
		for (i = 1; i <= 10; i++) {
			Job jobPageRank = Job.getInstance(getConf(), "pagerank");
			jobPageRank.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(jobPageRank,
					"/home/cloudera/tempOut1" + "job" + (i - 1));
			FileOutputFormat.setOutputPath(jobPageRank, new Path(
					"/home/cloudera/tempOut1" + "job" + i));
			jobPageRank.setMapperClass(MapPageRank.class);
			jobPageRank.setReducerClass(ReducePageRank.class);
			jobPageRank.setOutputKeyClass(Text.class);
			jobPageRank.setOutputValueClass(Text.class);
			jobPageRank.waitForCompletion(true);
			fileSystem.delete(new Path("/home/cloudera/tempOut1" + "job" + (i - 1)), true);
		}

		// job to sort the rank value
		Job jobSorting = Job.getInstance(getConf(), "pagerank");
		jobSorting.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(jobSorting, "/home/cloudera/tempOut1"
				+ "job" + (i - 1));
		FileOutputFormat.setOutputPath(jobSorting, new Path(args[1]));
		jobSorting.setMapperClass(MapSorting.class);
		jobSorting.setReducerClass(ReduceSorting.class);
		// creates only one reducer for jobSorting
		jobSorting.setNumReduceTasks(1);
		jobSorting.setMapOutputKeyClass(DoubleWritable.class);
		jobSorting.setMapOutputValueClass(Text.class);
		jobSorting.setOutputKeyClass(Text.class);
		jobSorting.setOutputValueClass(DoubleWritable.class);
		jobSorting
				.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		jobSorting.waitForCompletion(true);
		fileSystem.delete(new Path("/home/cloudera/tempOut1" + "job" + (i - 1)), true);

		return 1;

	}

	// Mapper to count the value of N
	public static class MapCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String lineString = lineText.toString();

			if (lineString != null && !lineString.isEmpty()) {
				Text pageTitle = new Text();
				Matcher matcherTitle = titlePattern.matcher(lineString);

				if (matcherTitle.find()) {
					
					pageTitle = new Text(matcherTitle.group(1).trim());
					
					titleList.add(matcherTitle.group(1).trim());
					context.write(pageTitle, new IntWritable(1));
					
				}
			}
		}
	}

	// Reducer to count the value of N
	public static class ReduceCounter extends Reducer<Text, IntWritable, Text, IntWritable> {

		int pageCounter = 0;

		@Override
		protected void reduce(Text title, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			this.pageCounter++;
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {

			context.getCounter("TotalPage", "TotalPage").increment(pageCounter);
		}
	}

	// Mapper to generate link graph
	public static class MapLink extends Mapper<LongWritable, Text, Text, Text> {

		private static final Pattern titlePattern = Pattern.compile("<title>(.*?)</title>");
		private static final Pattern textPattern = Pattern.compile("<text+\\s*[^>]*>(.*?)</text>");
		private static final Pattern outlinkPattern = Pattern.compile("\\[\\[.*?]\\]");

		Pattern linkPat = Pattern.compile("\\[\\[.*?]\\]");

		double nValue;

		public void setup(Context context) throws IOException, InterruptedException {
			nValue = context.getConfiguration().getDouble(N_VALUE, 1);
		}

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			String lineString = lineText.toString();
			if (lineString != null && !lineString.isEmpty()) {

				Text title = new Text();
				Text outlinksText = new Text();
				String outTextString = null;
				String outLinkString = null;

				Matcher matcherTitle = titlePattern.matcher(lineString);
				Matcher matcherOutlink = outlinkPattern.matcher(lineString);

				// Finding the title
				if (matcherTitle.find()) {
					title = new Text(matcherTitle.group(1).trim());
				}

				// Initial page rank value
				double prInit = (double) 1 / (nValue);

				StringBuilder sbOut = new StringBuilder("####" + prInit + "####");
				int countLink = 1;

				while (matcherOutlink != null && matcherOutlink.find()) {

					outLinkString = matcherOutlink.group().replace("[[", "").replace("]]", "");

					if (!outLinkString.isEmpty()) {

						if (countLink > 1)
							sbOut.append("@@@@" + outLinkString);
						else if (countLink == 1) {
							sbOut.append(outLinkString);
							countLink++;
						}
					}
				}

				outlinksText = new Text(sbOut.toString().trim());
				context.write(title, outlinksText);
			}
		}
	}

	// Reducer will not do anything. Just an identity function
	public static class ReduceLink extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text title, Text outlink, Context context) throws IOException, InterruptedException {

			context.write(title, outlink);
		}
	}

	// Mapper to calculate final page rank value for 10 iteration
	public static class MapPageRank extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

			float pageRankVal = 0.0f;
			String lineString = lineText.toString();
			String pageRankString = "";

			String[] values = lineString.split("####");
			float initPageRank = Float.parseFloat(values[1]);

			// For pages with no outlinks
			if (values.length < 3) {
				context.write(new Text(values[0].trim()), new Text("####" + values[1].trim()));
			}

			// For pages with 1 or more outlinks
			else if (values.length == 3) {
				String[] outLinks = values[2].split("@@@@");
				
				context.write(new Text(values[0].trim()),new Text("####"+values[1].trim()+"####"+values[2].trim()));

				// looping over the outlink and getting updated page rank value for all outlink pages
				for (int i = 0; i < outLinks.length; i++) {
					String outLinkTitle = outLinks[i].trim();

					// calculate the page ranks for out links
					pageRankVal = 0.85f * (initPageRank / (float) (outLinks.length));
					pageRankString = Float.toString(pageRankVal);

					// Filtering out the noisy pages before sending to reducer 
					if (titleList.contains(outLinkTitle.trim())) {
					
					context.write(new Text(outLinkTitle + "$$$$"), new Text("####" + pageRankString));
					}
				}
			}
		}
	}

	// Reducer to calculate final page rank value for 10 iteration
	public static class ReducePageRank extends Reducer<Text, Text, Text, Text> {

		static String tempKey = "";
		static String tempValue = "";

		@Override
		public void reduce(Text title, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String titleString = title.toString().trim();
			String pageRankString = "";
			String[] arrValue;
			float pageRank = 0.0f;

			if (!titleString.contains("$$$$")) {

				if (!tempKey.equals("")) {
					context.write(new Text(tempKey), new Text(tempValue));
				}

				for (Text value : values) {

					pageRankString = value.toString();
				}
				// storing titleString and pageRankString
				tempKey = titleString;
				tempValue = pageRankString;
			} else {
				// calculate the updated page rank for all outlinks
				String titleStringTr = titleString.substring(0, titleString.length() - 4).trim(); // storing the title

				for (Text value : values) {
					pageRankString = value.toString();
					pageRankString = pageRankString.substring(4);
					pageRank = pageRank + Float.parseFloat(pageRankString);
				}

				// For damping factor
				pageRank = pageRank + 0.15f; // Adding (1-d) = 0.15f factor to the page rank

				// Replacing the rank
				if (titleStringTr.equals(tempKey)) {
					arrValue = tempValue.split("####");

					if (arrValue.length > 2) {
						tempValue = "####" + pageRank + "####" + arrValue[2]; 
					} else {
						tempValue = "####" + pageRank;
					}

					tempKey = "";
					context.write(new Text(titleStringTr.trim()), new Text(tempValue.trim()));
				} else {

					if (!tempKey.equals("")) {
						context.write(new Text(tempKey.trim()), new Text(tempValue.trim()));
						tempKey = "";
					}

					context.write(new Text(titleStringTr.trim()), new Text("####" + Float.toString(pageRank)));
				}
			}
		}
	}

	// Mapper for sorting page rank value
	public static class MapSorting extends Mapper<LongWritable, Text, DoubleWritable, Text> {

		public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
			String lineString = lineText.toString();
			String[] arrayInput = lineString.split("####");
			double pageRank = Double.parseDouble(arrayInput[1]);

			context.write(new DoubleWritable(pageRank), new Text(arrayInput[0].trim()));
		}
	}

	// Reducer for sorting page rank value
	public static class ReduceSorting extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

		double totalSum = 0;

		public void reduce(DoubleWritable counts, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			double tempVal = 0;

			tempVal = counts.get();
			String title = "";

			for (Text value : values) {
				title = value.toString().trim();
				context.write(new Text(title), new DoubleWritable(tempVal));
			}

		}


	}

}
