package CloudCompu.hw1;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class RetvalNewReduce extends Reducer<Text, WordPos, Text, Text> {
	private Text Score = new Text();
	private Text KeyWeight = new Text();

	public void reduce(Text key, Iterable<WordPos> values, Context context)
			throws IOException, InterruptedException {

		String[] query = context.getConfiguration().get("query").split(" ");
		HashSet<String> h = new HashSet<String>();

		StringBuilder tmp = new StringBuilder("");
		double score = 0D;

		int ignore = Integer.parseInt(context.getConfiguration().get("ignore"));
		if (ignore == 0) {
			for (String q : query)
				h.add(q);

			for (WordPos val : values) {
				String[] str = val.toString().split(" ");

				if (h.contains(str[0])) {
					score += val.getW();
					tmp.append(str[1]);
					for (int i = 2; i < str.length; i++) {
						tmp.append(" " + str[i]);
					}

					tmp.append("_");
				}

			}
		} else {
			for (String q : query)
				h.add(q.toLowerCase());

			for (WordPos val : values) {
				String[] str = val.toString().split(" ");

				if (h.contains(str[0].toLowerCase())) {
					score += val.getW();
					tmp.append(str[1]);
					for (int i = 2; i < str.length; i++) {
						tmp.append(" " + str[i]);
					}

					tmp.append("_");
				}

			}
		}

		Score.set("" + score);
		KeyWeight.set(key.toString() + "~" + tmp);

		context.write(Score, KeyWeight);

	}
}
