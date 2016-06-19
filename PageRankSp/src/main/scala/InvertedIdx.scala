import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

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
    try { hdfs.delete(new Path("Hw3/preprocess"), true) } catch { case _: Throwable => {} }
    try { hdfs.delete(new Path("Hw3/ids2title"), true) } catch { case _: Throwable => {} }
    try { hdfs.delete(new Path("Hw3/title2ids"), true) } catch { case _: Throwable => {} }

    val lines = sc.textFile(filePath, sc.defaultParallelism * 3)

    val res = (lines.map(line => {
      val lineXml = scala.xml.XML.loadString(line.toString())
      ((lineXml \ "title").text, ((lineXml \ "revision" \ "text").text));
    }))
      .zipWithUniqueId().map(x => (x._2.toString(), x._1)).partitionBy(new HashPartitioner(sc.defaultParallelism * 3))
    
    res.map(x => (x._1, x._2._2)).saveAsNewAPIHadoopFile(outputPath,classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])
    //res.map(x => (x._1, x._2._1)).saveAsNewAPIHadoopFile("Hw3/ids2title",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])
    //res.map(x => (x._2._1, x._1)).saveAsNewAPIHadoopFile("Hw3/title2ids",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])
    
 
  //  res.map(x => (x._1 + "&gt&gt&gt&gt" + x._2._2)).saveAsTextFile(outputPath);
    res.map(x => (x._2._1+"`"+ x._2._2)).saveAsTextFile("Hw3/preprocess");
    res.map(x => (x._1 + "|" + x._2._1)).saveAsTextFile("Hw3/ids2title");
    res.map(x => (x._2._1 + "|" + x._1)).saveAsTextFile("Hw3/title2ids");
	
    sc.stop
  }
}