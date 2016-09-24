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

/**
 * 
 * @author Miao1
 *	数据格式：
 *	0029029007999991901052806004+64333+023450FM-12
 *	+000599999V0202701N002119999999N0000001N9+00331+99999100911ADDGF103999199999999999999
 *	关键字段
 *	190101020 代表了1901年10点20分采集
 *	String year = value.subString(15,18)
 *
 *	判断温度
 *	int airT
 *	if (line.charAt(87)) == '+'){
 *		airT = line.subString(88,92)
 *	}
 *	else{
 *		airT = line.subString(87,92)
 *	}
 */

public class Temperature {
	
	public static class tMap extends
	Mapper<LongWritable, Text, Text, IntWritable>{
		String year = null;
		int airT=0;
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			super.map(key, value, context);
			
			year = value.toString().substring(15,18);
			
			if(value.toString().charAt(87)=='+'){
				airT = Integer.parseInt(value.toString().substring(88, 92));
			}
			else{
				airT = Integer.parseInt(value.toString().substring(87, 92));
			}
			if (airT != 9999){  //if temperture is missing
				context.write(new Text(year), new IntWritable(airT));
			}
		}
		
//1901,<10,20,30>
//1902,<-10,30,20>
		public static class tReduce extends
		Reducer<Text, IntWritable, Text, IntWritable>{
			
			int maxT = Integer.MIN_VALUE;
			int sum = 0;
			int count = 0;
			@Override
			protected void reduce(Text arg0, Iterable<IntWritable> arg1,
					Reducer<Text, IntWritable, Text, IntWritable>.Context arg2)
					throws IOException, InterruptedException {
				// TODO Auto-generated method stub
				super.reduce(arg0, arg1, arg2);
				
				//求最大值
				for(IntWritable iw: arg1){
					maxT = Math.max(maxT,iw.get());
				}
				arg2.write(arg0,new IntWritable(maxT));
				
				/*//求平均值
				for(IntWritable iw: arg1){
					sum = sum + iw.get();
					count++;
				}
				arg2.write(arg0, new IntWritable(sum/count));*/
			}
			
			public static void main(String[] args) throws Exception {
				Configuration conf = new Configuration();
				@SuppressWarnings("deprecation")
				Job job = new Job(conf);
				job.setJarByClass(WordCount.class);
				job.setJobName("temperature");
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(IntWritable.class);
				job.setMapperClass(tMap.class);
				job.setReducerClass(tReduce.class);
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				
				conf.set("fs.hdfs.impl", 
				        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
				    );
				conf.set("fs.file.impl",
				        org.apache.hadoop.fs.LocalFileSystem.class.getName()
				    );
				    
				FileInputFormat.addInputPath(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				job.waitForCompletion(true);
			}
			
		}
	} 
}
