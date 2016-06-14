package CloudCompu.hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public class WordPos extends Text {
	private double w = 0;
	private int file_id = 0;

	public void setW(double W) {
		this.w = W;
	}

	public double getW() {
		return w;
	}

	public void setfile_id(int file_id) {
		this.file_id = file_id;
	}

	public int getfile_id() {
		return this.file_id;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeDouble(w);
		out.writeInt(file_id);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);

		w = in.readDouble();
		file_id = in.readInt();
	}
}
