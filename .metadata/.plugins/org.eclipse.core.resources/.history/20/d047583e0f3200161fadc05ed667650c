package CloudCompu.hw1;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class RetvalSortCompare extends WritableComparator {
	protected RetvalSortCompare() {
		super(Text.class, true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) {
		Text t1 = (Text) w1;
		Text t2 = (Text) w2;
		String[] s1, s2;
		s1 = t1.toString().split("_");
		s2 = t2.toString().split("_");
		return -1*Double.compare(Double.parseDouble(t1.toString()),
				Double.parseDouble(t2.toString()));
		/*int compare = s1[0].compareTo(s2[0]);
		if (compare == 0) {
			return Double.compare(Double.parseDouble(s1[1]),
					Double.parseDouble(s2[1]));
		} 
		return compare;*/
	}
}
