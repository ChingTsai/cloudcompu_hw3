package hw3.query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

class page implements Comparable<page> {
	public double tfdf = 0.0d;
	public LinkedList<word> wordsets;
	public String title;

	public page(String t, word w) {
		title = t;
		wordsets = new LinkedList<word>();
		wordsets.add(w);
	}

	public int compare(Object o1, Object o2) {

		page a = (page) o1;
		page b = (page) o2;
		return Double.compare(a.tfdf, b.tfdf);
	}

	public int compareTo(page o) {

		return -1 * Double.compare(this.tfdf, o.tfdf);
	}
}

class word implements Comparable<word> {
	public double tfdf = 0.0d;
	public String[] offset;
	public String word;

	public word(String t, String o, double s) {
		word = t;
		offset = o.split(" ");
		tfdf = s;
	}

	public int word(Object o1, Object o2) {

		word a = (word) o1;
		word b = (word) o2;
		return Double.compare(a.tfdf, b.tfdf);
	}

	public int compareTo(word o) {

		return -1 * Double.compare(this.tfdf, o.tfdf);
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

			Table invidx = connection.getTable(TableName
					.valueOf("s104062587:invidx"));
			Table pagerank = connection.getTable(TableName
					.valueOf("s104062587:pagerank"));
			Table ids2title = connection.getTable(TableName
					.valueOf("s104062587:ids2title"));
			// Table title2ids =
			// connection.getTable(TableName.valueOf("s104062587:title2ids"));
			Table preprocess = connection.getTable(TableName
					.valueOf("s104062587:preprocess"));
			long t1;

			HashMap<String, String> id2t = new HashMap<String, String>(100000,
					(float) 0.75);
			HashMap<String, String> PR = new HashMap<String, String>(100000,
					(float) 0.75);
			Scan allscan = new Scan();
			ResultScanner ss = ids2title.getScanner(allscan);
			t1 = System.currentTimeMillis();

			for (Result result = ss.next(); (result != null); result = ss
					.next()) {
				id2t.put(Bytes.toString(result.getRow()), Bytes.toString(result
						.getValue("title".getBytes(), null)));

			}
			ss = pagerank.getScanner(allscan);
			for (Result result = ss.next(); (result != null); result = ss
					.next()) {
				PR.put(Bytes.toString(result.getRow()),
						Bytes.toString(result.getValue("pr".getBytes(), null)));

			}
			System.out.println("Prebuild Time: "
					+ (System.currentTimeMillis() - t1));

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
			// query = args[0];
			System.out.print("Query> ");

			while ((query = in.readLine()) != null && query.length() != 0) {
				t1 = System.currentTimeMillis();
				System.out.println("");
				q = query.split(" ");
				Result result;
				for (String s : q) {

					result = invidx.get(new Get(s.getBytes()));
					df = Integer.parseInt(Bytes.toString(result.getValue(
							Bytes.toBytes("df"), null)));

					info = Bytes.toString(
							result.getValue(Bytes.toBytes("info"), null))
							.split(";");
					for (String i : info) {

						tmp = i.split(":");
						String tmpTitle = id2t.get(tmp[0]);
						// String tmpTitle = Bytes.toString(ids2title.get( new
						// Get(tmp[0].getBytes())).getValue("title".getBytes(),
						// null));

						if (H.containsKey(tmpTitle)) {
							page p = H.get(tmpTitle);
							p.wordsets.add(new word(s, tmp[2], Integer
									.parseInt(tmp[1]) * Math.log10(N / df)));

						} else {

							H.put(tmpTitle,
									new page(tmpTitle, new word(s, tmp[2],
											Integer.parseInt(tmp[1])
													* Math.log10(N / df))));
						}
					}

				}
				System.out.println("Query Time: "
						+ (System.currentTimeMillis() - t1));

				t1 = System.currentTimeMillis();
				for (page p : H.values()) {
					double tfdfsum = 0;
					for (word w : p.wordsets)
						tfdfsum += w.tfdf;

					p.tfdf = tfdfsum * Double.parseDouble(PR.get(p.title));

					// p.tfdf = tfdfsum * Double.parseDouble( Bytes.toString(
					// pagerank.get(new
					// Get(p.title.getBytes())).getValue("pr".getBytes(), null)
					// ));
					Collections.sort(p.wordsets);
				}
				System.out.println("Get Pagerank Time: "
						+ (System.currentTimeMillis() - t1));

				t1 = System.currentTimeMillis();
				ArrayList<page> valuesList = new ArrayList<page>(H.values());
				Collections.sort(valuesList);
				System.out.println("Sorting Time: "
						+ (System.currentTimeMillis() - t1));
				Matcher matcher;
				LinkedList<Integer> L = new LinkedList<Integer>();
				int count = 0;
				t1 = System.currentTimeMillis();
				for (int j = 0; j < 10 && j < valuesList.size(); j++) {
					page p = valuesList.get(j);
					System.out.println("No." + (j + 1) + " : " + p.title
							+ " [Score= " + p.tfdf + " ] ");
					String text = Bytes.toString(preprocess.get(
							new Get(Bytes.toBytes(p.title))).getValue(
							"text".getBytes(), null));
					matcher = Pattern.compile("([A-Za-z]+)").matcher(text);
					L.clear();

					for (word w : p.wordsets) {
						for (String s : w.offset) {
							int f = Integer.parseInt(s);
							if (L.size() == 0)
								L.add(f);
							else if (f != L.getLast())
								L.add(f);
							if (L.size() == 3)
								break;
						}
						if (L.size() == 3)
							break;
					}
					count = 0;
					Collections.sort(L);
					while (!L.isEmpty()) {
						int first = L.removeFirst();
						while (count < first) {
							matcher.find();
							count++;
						}
						int st = matcher.start();
						System.out.println("\t" + st + " : "
								+ text.substring(st, st + 50));
					}
					System.out.println("");
				}
				System.out.println("Output Time: "
						+ (System.currentTimeMillis() - t1));
				System.out.print("Query> ");
			}

			// Finalize and close connection to Hbase
			admin.close();
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}