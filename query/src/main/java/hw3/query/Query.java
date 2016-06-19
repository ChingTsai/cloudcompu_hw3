package hw3.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

class page implements Comparable<page> {
	public double tfdf = 0.0d;
	public LinkedList<String[]> offset;
	public String title;

	public page(String t, String o, double s) {
		title = t;
		offset = new LinkedList<String[]>();
		offset.add(o.split(" "));
		tfdf = s;
	}

	public int compare(Object o1, Object o2) {

		page a = (page) o1;
		page b = (page) o2;
		return Double.compare(a.tfdf, b.tfdf);
	}

	public int compareTo(page o) {

		return Double.compare(this.tfdf, o.tfdf);
	}
}

public class Query {
	/*
	 * API can be found here: https://hbase.apache.org/apidocs/
	 */

	private static Configuration conf;
	private static Connection connection;
	private static Admin admin;

	public static void main(String[] args) {

		try {
			// Instantiating hbase connection
			conf = HBaseConfiguration.create();
			connection = ConnectionFactory.createConnection(conf);
			admin = connection.getAdmin();

			// HTable invidx = new HTable(conf, args[0]);
			// HTable pagerank = new HTable(conf, args[1]);
			Table invidx = connection.getTable(TableName
					.valueOf("s104062587:100M"));
			Table pagerank = connection.getTable(TableName
					.valueOf("s104062587:pagerank"));
			Table ids2title = connection.getTable(TableName
					.valueOf("s104062587:ids2title"));
			Table title2ids = connection.getTable(TableName
					.valueOf("s104062587:title2ids"));

			/*
			 * invidx = new HTable(conf, "s104062587:100M"); HTable pagerank =
			 * new HTable(conf, "s104062587:pagerank"); HTable ids2title = new
			 * HTable(conf, "s104062587:ids2title"); HTable title2ids = new
			 * HTable(conf, "s104062587:title2ids");
			 */
			BufferedReader br = new BufferedReader(new FileReader("N.txt"));
			long N = Long.parseLong(br.readLine().trim());
			br.close();
			BufferedReader in = new BufferedReader(new InputStreamReader(
					System.in));
			String query;
			String[] q;
			int df;
			String[] info;
			String[] tmp;
			HashMap<String, page> H = new HashMap<String, page>();
			query = args[0];
			// while ((query = in.readLine()) != null && query.length() != 0) {

			q = query.split(" ");
			Result result;
			for (String s : q) {
				/*
				 * Get getid = new Get(Bytes.toBytes(s)); byte[] idbyte =
				 * title2ids.get(getid).getValue( Bytes.toBytes("id"),
				 * Bytes.toBytes(""));
				 * System.out.println(Bytes.toString(idbyte));
				 */
				result = invidx.get(new Get(s.getBytes()));
				df = Integer.parseInt(Bytes.toString(result.getValue(
						Bytes.toBytes("df"), null)));

				info = Bytes.toString(
						result.getValue(Bytes.toBytes("info"), null))
						.split(";");
				for (String i : info) {
					
					tmp = i.split(":");
					String tmpTitle = Bytes.toString(ids2title.get(
							new Get(Bytes.toBytes(tmp[0]))).getValue(
							Bytes.toBytes("title"), null));
					if (H.containsKey(tmpTitle)) {
						page p = H.get(tmpTitle);
						p.offset.add(tmp[2].split(" "));
						p.tfdf = p.tfdf + Integer.parseInt(tmp[1])
								* Math.log10(N / df);
					} else {

						H.put(tmpTitle,
								new page(tmpTitle, tmp[2], Integer
										.parseInt(tmp[1]) * Math.log10(N / df)));
					}
				}

				// H.put(s, invidx.get(new Get(Bytes.toBytes(s))));
			}
			for (page p : H.values()) {
				p.tfdf = p.tfdf
						* Double.parseDouble(Bytes.toString(pagerank.get(
								new Get(Bytes.toBytes(p.title))).getValue(
								"pr".getBytes(), null)));
			}

			ArrayList<page> valuesList = new ArrayList<page>(H.values());
			Collections.sort(valuesList);
			for (int j = 0; j < 4 && j < valuesList.size(); j++) {
				System.out.println(valuesList.get(j).title + " : "+ valuesList.get(j).tfdf);
			}

			// }

			// Finalize and close connection to Hbase
			admin.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}