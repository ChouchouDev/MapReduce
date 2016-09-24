//手机流量统计


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Flow {
	
	/**
	 * mobile,uppack,downpack,payload,downloadpayload
	 * 手机号码 上行流量，下行流量,上行总数，下行总数
	 * 13910001000 3L 3L 90L 40L 
	 * 13910001000 4L 3L 32L 23L
	 * 13910001000 1L 3L 90L 420L
	 * 存为text 的意义：
	 * 13910001000 3L-3L-30L-40L
	 */
	public static class fMap extends
	Mapper<LongWritable, Text, Text, Text>{
		String m= null;
		long uppack = 0L;
		long dpack = 0L;
		long up = 0L;
		long dp = 0L;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			String[] values = value.toString().split("\\t");
			m = values[1];
			uppack = Long.parseLong(values[1]);
			dpack = Long.parseLong(values[2]);
			up = Long.parseLong(values[3]);
			dp = Long.parseLong(values[4]);
			
			context.write(new Text(m),new Text(uppack+"-"+dpack+"-"+up+"-"+dp));
			
		}
	}
	
	// (13910001000,<1-2-3-4,2-3-4-5...>)
	public static class fReduce extends
	Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,
				Reducer<Text, Text, Text, Text>.Context arg2) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.reduce(arg0, arg1, arg2);
			long uppack = 0L;
			long dpack = 0L;
			long up = 0L;
			long dp = 0L;
			for (Text t:arg1){
				uppack += Long.parseLong(t.toString().split("-")[0]);
				dpack += Long.parseLong(t.toString().split("-")[1]);
				dp+= Long.parseLong(t.toString().split("-")[2]);
				dp += Long.parseLong(t.toString().split("-")[3]);
			}
			arg2.write(arg0,new Text(uppack+"\t"+dpack+"\t"+up+"\t"+dp));
		}
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("flow");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(fMap.class);
		job.setReducerClass(fReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
	
}
