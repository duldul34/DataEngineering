import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
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

public class UBERStudent20190981 {
	public static String dayToStr(int day) {
		switch (day) {
			case 1:
				return "MON";
			case 2:
				return "TUE";
			case 3:
				return "WED";
			case 4:
				return "THR";
			case 5:
				return "FRI";
			case 6:
				return "SAT";
			case 7:
				return "SUN";
			default:
				return "NAN";
		}
	}
	
	public static class UBERMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text word = new Text();
		Text value = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			StringTokenizer tokenizer  = new StringTokenizer(value.toString(), ",");
			if (tokenizer.countTokens() < 4) {
				return;
			}
			
			String baseNumber =  tokenizer.nextToken().trim();
			String date = tokenizer.nextToken().trim(); 
			int activeVehicles = Integer.parseInt(tokenizer.nextToken().trim());
			int trips = Integer.parseInt(tokenizer.nextToken().trim());
			int day = -1;
			try {
                                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
                                Date dateObj = sdf.parse(date);
                                Calendar cal = Calendar.getInstance();
                                cal.setTime(dateObj);
                                day = cal.get(Calendar.DAY_OF_WEEK);
				if (day == 1) {
					day = 7;
				}
				else {
					day -= 1;
				}
                        } catch(Exception e) {
                                e.printStackTrace();
                        }	

			word.set(baseNumber + "," + day);
			value.set(trips + "," + activeVehicles);
			
			context.write(word, value);
		}	
	}
	
	public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
		Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int activeVehiclesSum = 0;
			int tripsSum = 0;
			
			StringTokenizer tokenizer = new StringTokenizer(key.toString(), ",");
                        String baseNumber = tokenizer.nextToken().trim();
                        int day = Integer.parseInt(tokenizer.nextToken().trim());
                        String dayStr = dayToStr(day);
			key.set(baseNumber + "," + dayStr);
			
			for (Text val : values) {
				tokenizer = new StringTokenizer(val.toString(), ",");
				int trips = Integer.parseInt(tokenizer.nextToken().trim());
				int activeVehicles = Integer.parseInt(tokenizer.nextToken().trim());
				
				activeVehiclesSum += activeVehicles;
				tripsSum += trips;
			}
			
			result.set(tripsSum + "," + activeVehiclesSum);
			
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}

    		Job job = new Job(conf, "UBERStudent20190981");

    		job.setJarByClass(UBERStudent20190981.class);
		job.setMapperClass(UBERMapper.class);
		job.setReducerClass(UBERReducer.class);
		job.setCombinerClass(UBERReducer.class);
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
	
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
