package CloudCompu.hw1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class KeyDetial implements Writable {
	private HashMap<Integer, Integer> wrdMap;

	public KeyDetial() {
		wrdMap = new HashMap<Integer, Integer>();
	}

	public HashMap<Integer, Integer> getTable() {
		return wrdMap;
	}

	public void readFields(DataInput in) throws IOException {
		int mapSize = in.readInt();
		for (int i = 0; i < mapSize; i++) {
			wrdMap.put(in.readInt(), in.readInt());
		}
	}

	public void write(DataOutput out) throws IOException {
		Iterator iter = wrdMap.entrySet().iterator();
		// write in total word
		out.writeInt(wrdMap.size());
		while (iter.hasNext()) {
			Map.Entry<Integer, Integer> entry = (Map.Entry) iter.next();
			// write in key value pairs as key then value
			out.writeInt(entry.getKey());
			out.writeInt(entry.getValue());

		}

	}

}
