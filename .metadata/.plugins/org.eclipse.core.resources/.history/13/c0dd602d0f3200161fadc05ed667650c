package CloudCompu.hw1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvIdxMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
	private MapWritable map = new MapWritable();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String filename = fileSplit.getPath().getName();

		// Replace nonAlphabetic with space
		StringTokenizer itr = new StringTokenizer(value.toString().replaceAll(
				"[^a-zA-Z]", " "));

		while (itr.hasMoreTokens()) {
			String toProcess = itr.nextToken();

			word.set(toProcess);
			map.put(new Text(filename), new IntWritable(1));
			context.write(word, map);

		}

	}
}
