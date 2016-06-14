import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner

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

    val lines = sc.textFile(filePath, sc.defaultParallelism * 5)

    val regex = "\\[\\[(.+?)([\\|#]|\\]\\])".r;

    var st = System.nanoTime

    var link =
      lines.map(line => {
        val lineXml = scala.xml.XML.loadString(line.toString())
        (  ((lineXml \ "title").text +"&gt"+lineXml \ "revision" \ "text"));
      }).saveAsTextFile(outputPath);

    sc.stop
  }
}