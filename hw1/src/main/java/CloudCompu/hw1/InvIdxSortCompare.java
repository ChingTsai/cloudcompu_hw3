package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvIdxSortCompare extends WritableComparator {
	protected InvIdxSortCompare() {
		super(Text.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;
		String key1 = t1.toString().split("_")[0];
		String key2 = t2.toString().split("_")[0];
		if (key1.equals(key2)) {
			int file_Id1 = Integer.parseInt(t1.toString().split("_")[1]);
			int file_Id2 = Integer.parseInt(t2.toString().split("_")[1]);

			return file_Id1 - file_Id2;
		} else {
			return key1.compareTo(key2);
		}
	}

}
