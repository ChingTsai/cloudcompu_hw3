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
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat

object NewInvIdx {
  def main(args: Array[String]) {

    val filePath = args(0)

    val outputPath = args(1)

    val hconf = HBaseConfiguration.create()

    val conn = ConnectionFactory.createConnection(hconf)
    val userTable = TableName.valueOf("s104062587:invidx")
    val admin = conn.getAdmin;
    var tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("df".getBytes))
    tableDescr.addFamily(new HColumnDescriptor("info".getBytes))

    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)

    val conf = new SparkConf().setAppName("InvertedIdx_2ns_Phase")
    val sc = new SparkContext(conf)
    val regex = "([A-Za-z]+)".r;

    // Cleanup output dir
    val hadoopConf = sc.hadoopConfiguration
    var hdfs = FileSystem.get(hadoopConf)
    try { hdfs.delete(new Path(outputPath), true) } catch { case _: Throwable => {} }

    val lines = sc.newAPIHadoopFile(filePath, classOf[KeyValueTextInputFormat], classOf[Text], classOf[Text], new Configuration())
      .map { x => (x._1.toString(), x._2.toString()) }

    lines.cache();

    def convert(triple: (String, String, String)) = {
      val p = new Put(Bytes.toBytes(triple._1))
      p.addColumn(Bytes.toBytes("df"), null, Bytes.toBytes(triple._2))
      p.addColumn(Bytes.toBytes("info"), null, Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, p)
    }
    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "s104062587:invidx")

    var words = lines.map(line => {

      (regex.findAllIn(line._2).zipWithIndex).map(x => (x._1 + "&gt" + line._1, x._2))
    }).flatMap(y => y).groupByKey(sc.defaultParallelism * 3)
      .map(word => {
        val tmp = word._1.split("&gt")
        val offsets = word._2.toArray
        (tmp(0), (tmp(1).concat(":".concat(offsets.length.toString().concat(":".concat(offsets.mkString(" ")))))))
      })
      .groupByKey(sc.defaultParallelism * 3)
      .map(index => {
        val doc = index._2.toArray
        (index._1, doc.length.toString(), doc.mkString(";"))
      })
      .map(convert).saveAsHadoopDataset(jobConf);
      //.saveAsTextFile(outputPath)
      //val ids = sc.textFile("Hw3/ids2title", sc.defaultParallelism * 3).map(x => x.split("&gt")).map { x => (x(0), x(1)) }
      admin.close();
      sc.stop
  }
}