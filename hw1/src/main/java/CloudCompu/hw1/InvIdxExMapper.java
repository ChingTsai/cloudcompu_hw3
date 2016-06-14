package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvIdxExMapper extends
		Mapper<LongWritable, Text, Text, LongArrayWritable> {
	// private MapWritable map = new MapWritable();
	LongArrayWritable list = new LongArrayWritable();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		String filename = fileSplit.getPath().getName();

		Configuration conf = context.getConfiguration();
		String str = conf.get("allFile");
		StringTokenizer itr = new StringTokenizer(str);
		int fileId = 0, idx = 0;
		while (itr.hasMoreTokens()) {
			String s = itr.nextToken();
			if (s.equals(filename)) {
				fileId = idx;
			}
			idx++;
		}

		HashMap<String, LinkedList<LongWritable>> tmpMap = new HashMap<String, LinkedList<LongWritable>>();
		// Replace nonAlphabetic with space and split into token
		Matcher matcher = Pattern.compile("\\S+").matcher(
				value.toString().replaceAll("[^a-zA-Z]", " "));
		while (matcher.find()) {
			if (tmpMap.containsKey(matcher.group())) {
				tmpMap.get(matcher.group()).add(
						new LongWritable(key.get() + matcher.start()));
			} else {
				LinkedList<LongWritable> l = new LinkedList<LongWritable>();
				/*
				 * Store the offset of word by (this partition offset of the
				 * file and the word offset of this partition )
				 */
				l.add(new LongWritable(key.get() + matcher.start()));
				tmpMap.put(matcher.group(), l);
			}

		}

		for (Entry<String, LinkedList<LongWritable>> e : tmpMap.entrySet()) {
			word.set(e.getKey() + "_" + fileId);
			/*
			 * map.put(new Text("" + fileId), new
			 * LongArrayWritable((LongWritable[]) e.getValue() .toArray(new
			 * LongWritable[e.getValue().size()])));
			 */
			list.set((LongWritable[]) e.getValue().toArray(
					new LongWritable[e.getValue().size()]));
			list.setFileId(fileId);
			context.write(word, list);
		}

	}
}
