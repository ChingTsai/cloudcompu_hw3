package CloudCompu.hw1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Retrieval {
	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();

		conf1.set("query", args[3]);
		FileSystem fs = FileSystem.get(conf1);
		// get the FileStatus list from given dir
		FileStatus[] status_list = fs.listStatus(new Path(args[1]));
		int N = status_list.length;
		conf1.set("N", "" + N);
		
		conf1.set("ignore", args[4]);
		// Store global query for later usage
		Job job1 = Job.getInstance(conf1, "Retrieval");
		job1.setJarByClass(Retrieval.class);
		// Get input from key value pair
		job1.setInputFormatClass(KeyValueTextInputFormat.class);
		// set input format
		// setthe class of each stage in mapreduce
		job1.setMapperClass(RetvalMapper.class);
		job1.setPartitionerClass(RetvalPart.class);
		job1.setReducerClass(RetvalNewReduce.class);
		// job.setGroupingComparatorClass(RetvalGpCompare.class);
		// job.setCombinerClass(RetvalNewCombi.class);
		// job.setSortComparatorClass(RetvalSortCompare.class);

		// set the output class of Mapper and Reducer
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(WordPos.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		// set the number of reducer
		job1.setNumReduceTasks(8);

		// add input/output path
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("tmp"));

		job1.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		conf2.set("inputDir", args[1]);
		Job job2 = Job.getInstance(conf2, "Retrieval_2nd");
		job2.setJarByClass(Retrieval.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setMapperClass(Retval2ndMapper.class);
		job2.setPartitionerClass(RetvalPart.class);
		job2.setSortComparatorClass(RetvalSortCompare.class);
		job2.setGroupingComparatorClass(RetvalGpCompare.class);
		job2.setReducerClass(Retval2ndReduce.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(WordPos.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setNumReduceTasks(1);

		// add input/output path
		FileInputFormat.addInputPath(job2, new Path("tmp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		/*
		 * ControlledJob cjob1 = new ControlledJob(new Configuration());
		 * ControlledJob cjob2 = new ControlledJob(new Configuration());
		 * cjob1.setJob(job1); cjob2.setJob(job2); cjob2.addDependingJob(cjob1);
		 * JobControl jbcntrl=new JobControl("jbcntrl"); jbcntrl.addJob(cjob1);
		 * jbcntrl.addJob(cjob2); jbcntrl.run();
		 */

	}
}
