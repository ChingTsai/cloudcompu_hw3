package CloudCompu.hw1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(conf);
		// get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(args[0]));
		String allFile = "";
		if (status_list != null) {
			for (FileStatus status : status_list) {

				allFile = allFile + " " + status.getPath().getName();
			}
		}

		conf.set("allFile", allFile);
		Job job = Job.getInstance(conf, "InvertedIndex");
		job.setJarByClass(InvertedIndex.class);
		// set input format

		// job.setInputFormatClass(KeyValueTextInputFormat.class);
		/*
		 * Test String : 1,AG 3,BB TextInputFormat: (0,AG) (16,BB)
		 * KeyValueTextInputFormat: (1,AG) (3,BB) Can be config with
		 * mapreduce.input.keyvaluelinerecordreader.key.value.separato
		 */
		// setthe class of each stage in mapreduce
		job.setMapperClass(InvIdxExMapper.class);
		job.setPartitionerClass(InvIdxPart.class);
		job.setReducerClass(InvIdxExReducer.class);
		job.setCombinerClass(InvIdxExCombi.class);
		job.setGroupingComparatorClass(InvIdxGpCompare.class);
		job.setSortComparatorClass(InvIdxSortCompare.class);
		// job.setMapperClass(xxx.class);
		// job.setPartitionerClass(xxx.class);
		// job.setSortComparatorClass(xxx.class);
		// job.setReducerClass(xxx.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setOutputKeyClass(xxx.class);
		// job.setOutputValueClass(xxx.class);

		// set the number of reducer
		job.setNumReduceTasks(8);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
