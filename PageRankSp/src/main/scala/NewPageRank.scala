import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import java.io.PrintWriter
import java.io.File
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

object NewPageRank {
  def main(args: Array[String]) {

    val filePath = args(0)

    val outputPath = args(1)
    val hconf = HBaseConfiguration.create()

    val conn = ConnectionFactory.createConnection(hconf)
    val userTable = TableName.valueOf("s104062587:pagerank")
    val admin = conn.getAdmin;
    var tableDescr = new HTableDescriptor(userTable)
    tableDescr.addFamily(new HColumnDescriptor("pr".getBytes))

    if (admin.tableExists(userTable)) {
      admin.disableTable(userTable)
      admin.deleteTable(userTable)
    }
    admin.createTable(tableDescr)
    val conf = new SparkConf().setAppName("Page Rank Spark")
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
        val title = (lineXml \ "title").text;

        val out = regex.findAllIn(lineXml.text).toArray
          .map { x => x.replaceAll("[\\[\\]]", "").split("[\\|#]") }
          .filter { _.length > 0 }.map(_.head.capitalize);

        out.map { x => (x, title) }.+:(title, "&gt")
      }).flatMap(y => y).groupByKey(sc.defaultParallelism * 5).filter(_._2.exists { _ == "&gt" })
        //
        .map(row => {
          row._2.toArray.filter(_ != "&gt").map(tp => (tp, row._1)).+:(row._1, "&gt");
        }).flatMap(y => y).groupByKey(sc.defaultParallelism * 5).map(x => (x._1, x._2.toArray.filter { _ != "&gt" }))
    link = link.partitionBy(new HashPartitioner(sc.defaultParallelism * 5));

    link = link.cache();

    val n = lines.count();
    val alpha = 0.85;

    var micros = (System.nanoTime - st) / 1000000000.0
    System.out.println("Parse :  %1.5f seconds".format(micros));

    var rddPR = link.map(x => (x._1, 1.0 / n));

    var Err = 1.0;
    var iter = 0;

    rddPR = rddPR.cache();

    while (Err > 0.001) {
      st = System.nanoTime
      val con = link.join(rddPR, sc.defaultParallelism * 5);
      val dangpr = con.filter(_._2._1.length == 0).map(_._2._2).reduce(_ + _) / n * alpha;
      System.out.println("Dangl :  %1.10f ".format(dangpr));

      var tmpPR = con.map(row => {

        row._2._1.map { tp => (tp, row._2._2 / row._2._1.length * alpha) }.+:(row._1, 1.0 / n * (1 - alpha) + dangpr);

      }).flatMap(y => y).reduceByKey(_ + _);

      //var Er = tmpPR.subtractByKey(rddPR.map(x => (x._1, x._2._2))).partitionBy(partitioner)
      Err = tmpPR.join(rddPR, sc.defaultParallelism * 10).map(x => (x._2._1 - x._2._2).abs).reduce(_ + _)

      rddPR = tmpPR;

      micros = (System.nanoTime - st) / 1000000000.0
      System.out.println("Iteration : " + iter + " err: " + Err);
      System.out.println("Compute :  %1.5f seconds".format(micros))
      iter = iter + 1;
    }
    def convert(triple: (String, String)) = {
      val p = new Put(Bytes.toBytes(triple._1))
      p.addColumn(Bytes.toBytes("pr"), null, Bytes.toBytes(triple._2))
      (new ImmutableBytesWritable, p)
    }
    val jobConf = new JobConf(hconf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "s104062587:pagerank")
    val res = rddPR;
    res.sortBy({ case (page, pr) => (-pr, page) }, true, sc.defaultParallelism * 5)
      .map(x => (x._1, x._2.toString())).map(convert).saveAsHadoopDataset(jobConf);
    //.map(x => x._1 + "|" + x._2).saveAsTextFile(outputPath);
    admin.close();
    sc.stop
    val pw = new PrintWriter(new File("N.txt"))
    pw.write(n.toString());
    pw.close
  }
}