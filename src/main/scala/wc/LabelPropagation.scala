package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter

object LabelPropagationMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.LabelPropagationMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val k = 100
    val ITERATIONS = 10
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    val textFile = sc.textFile(args(0))
    // counts is map of node -> rank of node when counting edges involving node, ex if node 9 has the most edges = (9, 1)
    val counts = textFile.flatMap(line => line.split("  "))
                 .map(word => (word.asInstanceOf[Int], 1))
                 .reduceByKey(_ + _)
                 .sortBy(line => line._2)
                 .zipWithIndex()
                 .map(line => (line._1._1, line._2))
    // expensive operation to "rename" each node according to its ranking by edges
    var edges = textFile.map(line => (line.split("  ")(0).asInstanceOf[Int], line.split("  ")(1).asInstanceOf[Int]))
                 .join(counts)
                 .map(line => (line._2._1, line._2._2))
                 .join(counts)
                 .map(line => (line._2._2.asInstanceOf[Int], line._2._1.asInstanceOf[Int]))
    // expensive operation to "rename" each node according to its ranking by edges
    var nodes = textFile.flatMap(line => line.split("  "))
                 .distinct()
                 .map(word => (word.asInstanceOf[Int], word.asInstanceOf[Int]))
                 .join(counts)
                 .map(line => (line._2._2.asInstanceOf[Int], line._2._2.asInstanceOf[Int]))
    val stop = false
    while (!stop) {
      // each row is outgoing edge, (node, current label)
      val outgoingEdges = nodes.join(edges).map(line => (line._2._2, (line._1, line._2._1)))
      // each row is (node, current label), label of node at outgoing edge
      val assertingLabels = outgoingEdges.join(nodes).map(line => ((line._2._1._1, line._2._1._2), line._2._2))
                                .reduceByKey((a, b) => if (a < b) a else b)
    }
    //counts.saveAsTextFile(args(1))
  }
}