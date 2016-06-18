import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.mapreduce.{ InputFormat => NewInputFormat, Job => NewHadoopJob }
import org.apache.hadoop.io.{ LongWritable, MapWritable, Text, BooleanWritable }
import java.util.regex.Matcher

object NewInvIdx {
  def main(args: Array[String]) {

    val filePath = args(0)

    val outputPath = args(1)

    val conf = new SparkConf().setAppName("InvertedIdx_2ns_Phase")
    val sc = new SparkContext(conf)
    val regex = "([A-Za -z]+)".r;

    // Cleanup output dir
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(outputPath), true) } catch { case _: Throwable => {} }
    //val data = sc.newAPIHadoopFile(filePath,classOf[TextInputFormat],classOf[LongWritable],classOf[Text],new Configuration());

    val lines = sc.textFile(filePath, sc.defaultParallelism * 3).map(x => x.split("&gt"))
    .map { x => x.length }
    /*.map { x => (x(0), x(1)) }
    lines.cache();
    // def parse(x:String) =
    regex.findAllIn("dd").toArray
    var words = lines.map(line => {

      (regex.findAllIn(line._2).zipWithIndex).map(x => (x._1 + "&gt" + line._1, x._2))
    }).flatMap(y => y).groupByKey(sc.defaultParallelism * 3)
      .map(word => {
        val tmp = word._1.split("&gt")
        val offsets = word._2.mkString(",")
        (tmp(0), (tmp(1).concat("|".concat(offsets.length.toString().concat("|".concat(offsets))))))
      })
      .groupByKey(sc.defaultParallelism * 3)
      .map(index => {
        val doc = index._2.toArray
        (index._1+"&gt"+doc.length.toString()+"&gt"+doc.mkString(";"))
      })*/
      .saveAsTextFile(outputPath)
    //val ids = sc.textFile("Hw3/ids2title", sc.defaultParallelism * 3).map(x => x.split("&gt")).map { x => (x(0), x(1)) }

    sc.stop
  }
}