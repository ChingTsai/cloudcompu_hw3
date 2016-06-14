package CloudCompu.hw1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxCombi extends Reducer<Text, IntWritable, Text, MapWritable> {
	private MapWritable map = new MapWritable();
	private Text word = new Text();
	private Text file = new Text();
	private IntWritable count = new IntWritable();
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val: values) {
			sum += val.get();
		}
		String[] file_word = key.toString().split("_");
		word.set(file_word[0]);
		file.set(file_word[1]);
		count.set(sum);
		
		map.put(file, count);
		context.write(word,map);
		
	}
}
