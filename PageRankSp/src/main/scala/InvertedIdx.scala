import org.apache.spark.SparkConf

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
  override def generateActualKey(key: Any, value: Any): Any =
    NullWritable.get()

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]
}

object InvertedIdx {
  def main(args: Array[String]) {

    val filePath = args(0)

    val outputPath = args(1)

    val conf = new SparkConf().setAppName("InvertedIdx")
    val sc = new SparkContext(conf)

    // Cleanup output dir
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(outputPath), true) } catch { case _: Throwable => {} }

    val lines = sc.textFile(filePath, sc.defaultParallelism * 10)

    val cons = lines.count();
    val ids = (1 to cons.toInt);
    val RddIds = sc.parallelize(ids, sc.defaultParallelism);

    val res = RddIds.zip(lines.map(line => {
      val lineXml = scala.xml.XML.loadString(line.toString())
      (((lineXml \ "title").text.concat("&gt").concat((lineXml \ "revision" \ "text").text)));
    }))
    res.map(x => ("file" + x._1.toString() + ".txt", x._2)).partitionBy(new HashPartitioner(3)).saveAsHadoopFile(outputPath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])

    sc.stop
  }
}