package CloudCompu.hw1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxExCombi extends
		Reducer<Text, LongArrayWritable, Text, LongArrayWritable> {
	private LongArrayWritable list = new LongArrayWritable();

	public void reduce(Text key, Iterable<LongArrayWritable> values,
			Context context) throws IOException, InterruptedException {
		ArrayList<LongWritable> tmplist = new ArrayList<LongWritable>();

		for (LongArrayWritable val : values) {
			list.setFileId(val.getFileId());
			LongWritable[] offsets = (LongWritable[]) val.toArray();
			for (LongWritable o : offsets) {
				tmplist.add(o);
			}
		}

		list.set(tmplist.toArray(new LongWritable[tmplist.size()]));
		context.write(key, list);

	}
}
