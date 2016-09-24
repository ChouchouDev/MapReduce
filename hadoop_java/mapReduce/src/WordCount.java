
/**
运行设置
hdfs://localhost:9000/user/Miao1/forTest hdfs://localhost:9000/user/Miao1/outWordCount
*/

/**
 * hello world
 * hello kitty
 * 
 * 输入：文本文件 目录
 * 目录：默认就是笨目录下的所有的文件
 * LongWritable 是代表行号，偏移量offset
 * 0 hello world
 * 11 hello kitty
 * 
 * Text 代表的是每一行的数据
 * hello world
 * hello kitty
 * 
 * 输出：
 * Text IntWritable
 * 
 * 
 * 
 */

import java.io.IOException;
import java.util.StringTokenizer;
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

public class WordCount {
	public static class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		/**
		 * hello 1 world 1 hello 1 kitty 1
		 * 
		 * 自动shuffle之后: hello,<1,1> world,<1> kitty,<1>
		 * 
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer token = new StringTokenizer(line);
			while (token.hasMoreTokens()) {
				word.set(token.nextToken());
				context.write(word, one);
			}
			// 另外一种方式
			/*
			 * String[] values = value.toString().split(" "); String word=null;
			 * for(int i=0; i<values.length;i++){ word = values[i].toString();
			 * context.write(new Text(word), new IntWritable(i)); }
			 */
		}
	}

	/**
	 * 
	 * Reducer的输入参数 -- map的输出参数 shuffle: hello,<1,1> world,<1> kitty,<1>
	 *
	 */
	public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		@SuppressWarnings("deprecation")
		Job job = new Job(conf);
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(WordCountMap.class);
		job.setReducerClass(WordCountReduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
