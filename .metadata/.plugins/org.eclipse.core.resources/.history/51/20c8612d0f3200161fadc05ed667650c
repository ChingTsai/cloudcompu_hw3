package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class InvIdxReducer extends Reducer<Text, MapWritable, Text, Text> {
	private Text detail = new Text();

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
		String detString = tmpMap.size() + " > ";
		Iterator iter = tmpMap.entrySet().iterator();
		/*
		 * while (iter.hasNext()) { Map.Entry<String, Integer> entry =
		 * (Entry<String, Integer>) iter .next(); detString = entry.getKey() +
		 * ":" + entry.getValue(); }
		 */
		SortedSet<String> keys = new TreeSet<String>(tmpMap.keySet());
		for (String file : keys) {
			detString = detString + file + " : " + tmpMap.get(file) + ", ";
		}
		/*
		 * for (Entry<String, Integer> entry : tmpMap.entrySet()) { detString =
		 * detString + entry.getKey() + " : " + entry.getValue() + ", "; }
		 */
		detail.set(detString);
		context.write(key, detail);
	}
}
