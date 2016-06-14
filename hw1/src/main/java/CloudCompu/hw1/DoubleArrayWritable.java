package CloudCompu.hw1;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class DoubleArrayWritable extends ArrayWritable {
	/*
	 * Any type of Writable should implement this function below or you will get
	 * java.lang.NoSuchMethodException X.X.X.<init>()
	 */
	public DoubleArrayWritable() {
		super(DoubleWritable.class);
	}

	public DoubleArrayWritable(DoubleWritable[] doubles) {
		super(DoubleWritable.class);
		set(doubles);
	}
}
