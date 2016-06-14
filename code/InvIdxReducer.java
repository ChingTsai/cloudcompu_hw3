package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxReducer extends Reducer<Text, MapWritable, Text, Text> {
	private Text detail = new Text();

	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		for (MapWritable val : values) {
			Iterator iter = val.entrySet().iterator();
			String tmp_word;
			while (iter.hasNext()) {
				Map.Entry<Text, IntWritable> entry = (Map.Entry) iter.next();
				tmp_word = entry.getKey().toString();
				if (map.containsKey(tmp_word)) {
					map.put(tmp_word, map.get(tmp_word) + entry.getValue().get());
				} else {
					map.put(tmp_word, entry.getValue().get());
				}

			}
		}

		detail.set(map.toString());
		context.write(key,detail);
	}
}
