package CloudCompu.hw1;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Retval2ndReduce extends Reducer<Text, WordPos, Text, Text> {
	private Text detail = new Text();

	public void reduce(Text key, Iterable<WordPos> values, Context context)
			throws IOException, InterruptedException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String inputDir = context.getConfiguration().get("inputDir");
		FileStatus[] status_list = fs.listStatus(new Path(inputDir));

		int reducerId = context.getTaskAttemptID().getTaskID().getId();
		StringBuilder detString = new StringBuilder("\r\n");
		int file_id;
		int subRank = 1;
		Path inFile;
		byte[] buffer = new byte[50];
		long pos;
		for (WordPos val : values) {
			if (val.getW() > 0D) {
				file_id = val.getfile_id();
				detString.append("Rank " + (subRank + reducerId) + ": ");
				detString.append(status_list[file_id].getPath().getName());
				detString.append(" score = " + Double.toString(val.getW())
						+ "\r\n");
				detString.append("************************\r\n");
				subRank++;
				inFile = status_list[file_id].getPath();
				String[] offsetList = val.toString().split("_");

				for (int i = 0; i < offsetList.length; i++) {
					StringTokenizer itr = new StringTokenizer(offsetList[i]);
					while (itr.hasMoreTokens()) {
						pos = Long.parseLong(itr.nextToken());
						fs.open(inFile).read(pos - 25L, buffer, 0, 50);
						detString
								.append("   "
										+ pos
										+ "   "
										+ (new String(buffer, Charset
												.forName("UTF-8"))).replaceAll(
												"\r|\n", ""));
						detString.append("\r\n");
					}
				}

				detString.append( "************************\r\n");

			}
			if (subRank > 10)
				break;
		}

		detail.set(detString.toString());
		context.write(key, detail);
	}
}
