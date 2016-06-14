package CloudCompu.hw1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class InvIdxGpCompare extends WritableComparator {
	protected InvIdxGpCompare() {
		super(Text.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;
		return t1.toString().split("_")[0]
				.compareTo(t2.toString().split("_")[0]);

	}
}
