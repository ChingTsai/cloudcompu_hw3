package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvIdxPart extends Partitioner<Text, LongArrayWritable> {
	public int getPartition(Text key, LongArrayWritable value,
			int numReduceTasks) {
		double c = key.charAt(0);
		double a = 'a';
		double A = 'A';
		int part = numReduceTasks / 2;
		if (c < a) {
			return (int) (part * ((c - A) / 26));
		} else {
			return (int) (part * ((c - a) / 26) + part);
		}

	}
}
