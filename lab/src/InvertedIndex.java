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

public class InvertedIndex 
{
	public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
		private String filename;
		private Text word = new Text();
		private Text index = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			long offset = ((LongWritable) key).get();
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			long local_offset = 0;
			while(tokenizer.hasMoreTokens()) {
				String w = tokenizer.nextToken();
				word.set(w);
				
				index.set(filename + " : " + (offset + local_offset));
				
				context.write(word, index);
				
				local_offset += (w.length() + 1);
			}
		}
	
		protected void setup(Context context) 
				throws IOException, InterruptedException {
			filename = ((FileSplit) context.getInputSplit()).getPath().getName();
		}
	}
	
	public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text>
	{
		private Text index = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			String v = "";
			for (Text value : values) {
				v += value.toString() + " ";
			}
			index.set(v);
			context.write(key, index);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: InvertedIndex <in> <out>");
			System.exit(2);
		}


		Job job = new Job(conf, "inverted index");
		job.setJarByClass(InvertedIndex.class);
		job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
