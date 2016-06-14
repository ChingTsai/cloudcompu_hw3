package CloudCompu.hw1;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class RetvalPart extends Partitioner<Text, WordPos> {
	public int getPartition(Text key, WordPos value, int numReduceTasks) {

		int file_id = Integer.parseInt(key.toString());

		return 0;

	}
}
