package CloudCompu.hw1;


import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RetvalNewCombi extends Reducer<Text, WordPos, Text, WordPos> {
	private WordPos wp = new WordPos();
	private Text KeyWeight = new Text();

	public void reduce(Text key, Iterable<WordPos> values, Context context)
			throws IOException, InterruptedException {

		String[] query = context.getConfiguration().get("query").split(" ");
		HashSet<String> h = new HashSet<String>();
		for (String q : query)
			h.add(q);
		String tmp = "";
		double score = 0D;
		for (WordPos val : values) {
			String[] str = val.toString().split(" ");

			if (h.contains(str[0])) {
				score += val.getW();
				tmp = tmp + str[1];
				for (int i = 2; i < str.length; i++) {
					tmp = tmp + " " + str[i];
				}

				tmp = tmp + "_";
			}

		}

		wp.set(tmp);
		wp.setW(score);
		wp.setfile_id(Integer.parseInt(key.toString().split("_")[0]));
		KeyWeight.set("" + score);
		context.write(KeyWeight, wp);

	}
}
