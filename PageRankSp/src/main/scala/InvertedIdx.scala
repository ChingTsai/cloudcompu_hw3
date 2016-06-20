import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import java.io.PrintWriter
import java.io.File
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put

object InvertedIdx {
  def main(args: Array[String]) {

    val filePath = args(0)

    val outputPath = args(1)

    val hconf = HBaseConfiguration.create()

    val conn = ConnectionFactory.createConnection(hconf)
    var userTable = TableName.valueOf("s104062587:preprocess")
    val admin = conn.getAdmin;
    var tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("text".getBytes))

    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)
    userTable = TableName.valueOf("s104062587:ids2title")
    tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("title".getBytes))
    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)

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

    res.map(x => (x._1, x._2._2)).saveAsNewAPIHadoopFile(outputPath, classOf[Text], classOf[Text], classOf[TextOutputFormat[Text, Text]])
    //res.map(x => (x._1, x._2._1)).saveAsNewAPIHadoopFile("Hw3/ids2title",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])
    //res.map(x => (x._2._1, x._1)).saveAsNewAPIHadoopFile("Hw3/title2ids",classOf[Text],classOf[Text],classOf[TextOutputFormat[Text,Text]])

    //  res.map(x => (x._1 + "&gt&gt&gt&gt" + x._2._2)).saveAsTextFile(outputPath);
    val prep = res.map(x => (x._2._1, x._2._2))
    //prep.saveAsTextFile("Hw3/preprocess");
    val ids = res.map(x => (x._1, x._2._1))
    //.saveAsTextFile("Hw3/ids2title");
    //res.map(x => (x._2._1 + "|" + x._1)).saveAsTextFile("Hw3/title2ids");

    def convert(triple: (String, String)) = {
      val p = new Put(Bytes.toBytes(triple._1))
      p.addColumn(Bytes.toBytes("text"), null, Bytes.toBytes(triple._2))
      (new ImmutableBytesWritable, p)
    }
    def convertIds(triple: (String, String)) = {
      val p = new Put(Bytes.toBytes(triple._1))
      p.addColumn(Bytes.toBytes("title"), null, Bytes.toBytes(triple._2))
      (new ImmutableBytesWritable, p)
    }

    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "s104062587:preprocess")
    prep.map(convert).saveAsHadoopDataset(jobConf);
    
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "s104062587:ids2title")
    ids.map(convertIds).saveAsHadoopDataset(jobConf);
    
    admin.close();
    sc.stop

  }
}