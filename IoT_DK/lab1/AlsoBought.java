import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * Input text (Each line contains all the books that an user has bought):
 * A,B,C
 * A,B
 * A,B
 * A,C
 * 
 * Map output: key, map as value 
 * A => {(B:1), (C:1)}
 * B => {(A:1), (C:1)}
 * C => {(A:1), (B:1)}
 * A => {(B:1)}
 * B => {(A:1)}
 * A => {(B:1)}
 * A => {(B:1)}
 * B => {(A:1)}
 * A => {(B:1)}
 * C => {(A:1)}
 * 
 * Reduce output: compute the sum and output in order
 * A => {(B:3), (C:2)}
 * B => {(A:3), (C:1)}
 * C => {(A:2), (B:1)}
 */
public class AlsoBought {
	
	public static class AlsoBoughttMap extends Mapper<LongWritable, Text, Text, MyMapWritable> {
		Text item = new Text();
		MyMapWritable mapItems = new MyMapWritable();

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			Set<String> itemSet = new HashSet<>(); // avoid repeat item and eleminate space in the start and end
			for(String itemString:line.split(",")){
				itemSet.add(itemString.trim());
			}
			for (String item : itemSet) {
				mapItems = new MyMapWritable();
				for (String otherItem : itemSet) {
					if (!otherItem.equals(item)) {
						mapItems.put(new Text(otherItem), new IntWritable(1));
					}
				}
				context.write(new Text(item), mapItems);
			}
		}
	}

	public static class AlsoBoughtReduce extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
		@Override
		public void reduce(Text key, Iterable<MyMapWritable> maps, Context context)
				throws IOException, InterruptedException {
			MyMapWritable mapToWrite = new MyMapWritable();
			for(MyMapWritable map: maps) {
				for(Writable item: map.keySet()) {
					if(mapToWrite.containsKey((Text)item)) {
						IntWritable before = (IntWritable) mapToWrite.get(item);
						IntWritable toAdd = (IntWritable) map.get(((Text)item));
						mapToWrite.put((Text)item, new IntWritable(before.get()+toAdd.get()));
					} else {
						mapToWrite.put(item, (IntWritable) map.get(((Text)item)));
					}
				}
			}
			context.write(key, mapToWrite);
		}
	}
	
	/*
	 *  MapWritable's toString() function is not that good and the items in the MapWritable are not ordered.  Therefore I define a new class MyMapWritable.
	 */
	public static class MyMapWritable extends MapWritable{
		@SuppressWarnings("unchecked")
		@Override
		public String toString() {
			ValueComparator cp = new ValueComparator(this);
			TreeMap<Writable, Writable> sorted_map = new TreeMap<Writable, Writable>(cp);
			sorted_map.putAll(this);
			return sorted_map.toString();
		}
	}
	
	/*
	 * 	Comparable class for TreeMap to sort the map by values
	 */
	public static class ValueComparator implements Comparator<Writable> {
	    Map<Writable, Writable> base;
	    public ValueComparator(MyMapWritable myMapWritable) {
	    	this.base = myMapWritable;
		}
		@Override
		public int compare(Writable o1, Writable o2) {
			IntWritable v1 = (IntWritable)this.base.get(o1);
			IntWritable v2 = (IntWritable)this.base.get(o2);
			if (v1.get() >= v2.get()) 
	            return -1;
	        else 
	            return 1;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Customerswho bought this item also bought:");
		job.setJarByClass(AlsoBought.class);
		job.setMapperClass(AlsoBoughttMap.class);
		job.setReducerClass(AlsoBoughtReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MyMapWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
