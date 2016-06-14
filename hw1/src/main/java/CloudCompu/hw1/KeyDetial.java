package CloudCompu.hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class KeyDetial extends MapWritable {
	private int N = 0;
	
	public void N(int count) {
		this.N = count;
	}

	public int getN() {
		return this.N;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(N);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		N = in.readInt();
	}

}
