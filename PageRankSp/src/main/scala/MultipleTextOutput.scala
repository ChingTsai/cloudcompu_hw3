import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

/**
 * Leo Dj @2015-10-09
 */
object MultipleTextOutput {
def main(args: Array[String]) {
val filePath = "/user/input/operation.log";
val savePath = "/user/output/merge";
val conf = new SparkConf().setAppName("SplitTest")
val sc = new SparkContext(conf)

case class RDDMultipleTextOutputFormatter() extends MultipleTextOutputFormat[Any, Any] {
override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String ={
  val separator = ",";
key.asInstanceOf[String].split(separator)(72);
}
}
  sc.textFile(filePath).map(x=>(x,"")).partitionBy(new HashPartitioner(3)).saveAsHadoopFile(savePath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormatter])

}
}
