import java.util.*;
import java.io.*;
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

public class ProSel {
	public static class ProSelMapper extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		private Text result = new Text( new byte[0] );
		
		public void map(Object key, Text value, Context context) throws IOException,InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			if( itr.countTokens() < 4 ) return;
			
			String emp_id = itr.nextToken().trim();
			String dept_id = itr.nextToken().trim();
			int salary = Integer.parseInt( itr.nextToken().trim() );
			String emp_info = itr.nextToken().trim();
			if ( salary >= 3500000 ) {
				word.set( new byte[0] );
				word.set( emp_id + "|" + salary );
				context.write(word, result);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ProSel <in> <out>"); System.exit(2);
		}
		
		Job job = new Job(conf, "ProSel");
		job.setJarByClass(ProSel.class);
		job.setMapperClass(ProSelMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
