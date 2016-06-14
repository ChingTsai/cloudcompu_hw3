package CloudCompu.hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;

public class LongArrayWritable extends ArrayWritable {
	/*
	 * Any type of Writable should implement this function below or you will get
	 * java.lang.NoSuchMethodException X.X.X.<init>()
	 */
	private int fileId = 0;

	public void setFileId(int id) {
		this.fileId = id;
	}

	public int getFileId() {
		return this.fileId;
	}

	public LongArrayWritable() {
		super(LongWritable.class);
	}

	public LongArrayWritable(LongWritable[] longs) {
		super(LongWritable.class);
		set(longs);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(fileId);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		this.fileId = in.readInt();
	}
}
