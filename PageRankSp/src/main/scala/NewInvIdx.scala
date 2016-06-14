import org.apache.spark.SparkConf

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
object NewInvIdx {
    def main(args: Array[String]) {

    val filePath = args(0)

    val outputPath = args(1)

    val conf = new SparkConf().setAppName("InvertedIdx")
    val sc = new SparkContext(conf)
    val regex = "([A-Za -z]+)".r;
    // Cleanup output dir
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(outputPath), true) } catch { case _: Throwable => {} }

    val lines = sc.textFile(filePath, sc.defaultParallelism * 3)

   (lines.map(line => {
     
    }))
    
    sc.stop
  }
}