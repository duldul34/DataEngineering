import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class DoubleString implements WritableComparable {
	String joinKey = new String();
	String tableName = new String();
	
	public DoubleString() {}
	public DoubleString( String _joinKey, String _tableName )
	{
		joinKey = _joinKey;
		tableName = _tableName;
	}

	public void readFields(DataInput in) throws IOException
	{
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}
		
	public int compareTo(Object o1)
	{
		DoubleString o = (DoubleString) o1;
		
		int ret = joinKey.compareTo( o.joinKey );
		if (ret!=0) return ret;
		
		return -1 * tableName.compareTo( o.tableName );
	}

	public String toString() { return joinKey + " " + tableName; }
}

public class ReduceSideJoin2 {

	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			int result = k1.joinKey.compareTo(k2.joinKey);

			if (0 == result) {
				result = -1* k1.tableName.compareTo(k2.tableName);
			}
			
			result = k1.compareTo(k2);
			return result;
		}
	}
	
	
	public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
		public int getPartition(DoubleString key, Text value, int numPartition)
		{
			return key.joinKey.hashCode() % numPartition;
		}
	}
	
	public static class FirstGroupingComparator extends WritableComparator {
		protected FirstGroupingComparator()
		{
			super(DoubleString.class, true);
		}
		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			return k1.joinKey.compareTo(k2.joinKey);
		}
	}
		
	public static class ReduceSideJoin2Mapper extends Mapper<Object, Text, DoubleString, Text> {
		boolean fileA = true;

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			DoubleString outputKey = null;
			Text outputValue = new Text();
			
			if (fileA) {
				String productId = itr.nextToken();
				String productPrice = itr.nextToken();
				String productCode = itr.nextToken();

				outputKey = new DoubleString(productCode, "A");
				outputValue.set(productId + "," + productPrice);
			}
			else {
				String productCode = itr.nextToken();
				String productDescription = itr.nextToken();
				
				outputKey = new DoubleString(productCode, "B");
				outputValue.set(productDescription);
			}

			context.write(outputKey, outputValue);
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();

			if (filename.indexOf("relation_a") != -1) {
				fileA = true;
			}
			else {
				fileA = false;
			}	
		}
	}

	public static class ReduceSideJoin2Reducer extends Reducer <DoubleString,Text,Text,Text> {
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Text reduceKey = new Text();
			Text reduceVal = new Text();

			boolean isFirst = true;
			String description = "";
			
			for (Text val : values) {
				if (isFirst) {
					description = val.toString();
					isFirst = false;
				}
				else {
					String strVal = val.toString();
					String [] splitedVal = strVal.split(",");
					
					String productId = splitedVal[0];
					String productPrice = splitedVal[1];
					
					reduceKey.set(productId);
					reduceVal.set(productPrice + " " + description);
					
					context.write(reduceKey, reduceVal);
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ReduceSideJoin2 <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "ReduceSideJoin2");
		job.setJarByClass(ReduceSideJoin2.class);
		job.setMapperClass(ReduceSideJoin2Mapper.class);
		job.setReducerClass(ReduceSideJoin2Reducer.class);
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);

		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
