package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxCombi extends Reducer<Text, MapWritable, Text, MapWritable> {
	private MapWritable map = new MapWritable();

	public void reduce(Text key, Iterable<MapWritable> values, Context context)
			throws IOException, InterruptedException {
		HashMap<String, Integer> tmpMap = new HashMap<String, Integer>();

		for (MapWritable val : values) {
			Iterator<Entry<Writable, Writable>> iter = val.entrySet()
					.iterator();
			String tmpWord;
			while (iter.hasNext()) {
				Map.Entry<Text, IntWritable> entry = (Map.Entry) iter.next();
				tmpWord = entry.getKey().toString();
				if (tmpMap.containsKey(tmpWord)) {
					tmpMap.put(tmpWord, tmpMap.get(tmpWord)
							+ entry.getValue().get());
				} else {
					tmpMap.put(tmpWord, entry.getValue().get());
				}

			}
		}

		Iterator<Entry<String, Integer>> iter = tmpMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, Integer> entry = (Map.Entry) iter.next();

			map.put(new Text(entry.getKey()),
					new IntWritable(entry.getValue()));
		}
		context.write(key, map);

	}
}
