import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;

public class IMDB
{
	public static class IMDBMapper extends Mapper<LongWritable, Text, Text, LongWritable> 
	{
		private final LongWritable one = new LongWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("::");
			if (split.length < 3) {
				return;
			}
			
			String genre = split[split.length - 1];
			
			StringTokenizer tokenizer = new StringTokenizer(genre, "|");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
		
	}
	
	public static class IMDBReducer extends Reducer<Text, LongWritable, Text, LongWritable>
	{
		private LongWritable sumWritable = new LongWritable();
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			
			sumWritable.set(sum);
			context.write(key, sumWritable);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
	
		Job job = new Job(conf, "IMDB");

		job.setJarByClass(IMDB.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setCombinerClass(IMDBReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
	
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));	
    	
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
