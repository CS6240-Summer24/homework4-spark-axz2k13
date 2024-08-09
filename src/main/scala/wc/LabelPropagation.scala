package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
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
    val MAX = 100000
		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    val textFile = sc.textFile(args(0))
    // counts is map of node -> rank of node when counting edges involving node, ex if node 9 has the most edges = (9, 1)
    val counts = textFile.flatMap(line => line.split("\t"))
                 .map(word => (word.toInt, 1))
                 .filter(line => line._1 <= MAX || MAX < 0)
                 .reduceByKey(_ + _)
                 .sortBy(line => line._2)
                 .zipWithIndex()
                 .map(line => (line._1._1, line._2))
    //counts.saveAsTextFile(args(1))
    // expensive operation to "rename" each node according to its ranking by edges
    var edges = textFile.map(line => (line.split("\t")(0).toInt, line.split("\t")(1).toInt))
                 .filter(line => MAX < 0 || (line._1 <= MAX && line._2 <= MAX))
                //  .join(counts) // counts line
                //  .map(line => (line._2._1, line._2._2)) // counts line
                //  .join(counts) // counts line
                //  .map(line => (line._2._1.toInt, line._2._2.toInt)) // counts line
                 .flatMap(line => Seq(line, (line._2, line._1)))
                 .persist() // we reuse this RDD every iteration
    // expensive operation to "rename" each node according to its ranking by edges
    var nodes = textFile.flatMap(line => line.split("\t"))
                 .distinct()
                 .map(word => (word.toInt, word.toInt))
                 .filter(line => line._1 <= MAX || MAX < 0)
                //  .join(counts) // counts line
                //  .map(line => (line._2._2.toInt, line._2._2.toInt)) // counts line
    var stop = false
    var iterations = 0
    while (!stop) {
      stop = true
      iterations += 1
      val changes = sc.longAccumulator(iterations.toString)
      // each row is outgoing edge, (node, current label)
      val outgoingEdges = nodes.join(edges).map(line => (line._2._2, (line._1, line._2._1)))
      
      val assertingLabels = outgoingEdges.join(nodes).map(line => (line._2._1, line._2._2))
                                .reduceByKey((a, b) => if (a > b) a else b)
                                .map(line => {
                                  if (line._2 > line._1._2) {
                                    changes.add(1)
                                    //System.out.println("Change found")
                                    (line._1._1, line._2)
                                  } else {
                                    line._1
                                  }
                                })
      val foo = assertingLabels.collect() // we need to trigger it now for stop to update
      nodes = assertingLabels
      stop = changes.isZero
      System.out.println("changes value")
      System.out.println(changes.value)
    }
    // save output groups
    val componentSize = nodes.map(line => (line._2, 1)).reduceByKey(_ + _)
    val componentEdges = nodes.join(edges).map(line => (line._2._1, 1)).reduceByKey(_ + _)
    // components is component label, (size, # of edges)
    val components = componentSize.join(componentEdges)
                                  // .join(counts.map(line => (line._2.toInt, line._1))) // counts line
                                  // .map(line => (line._2._2, (line._2._1._1, line._2._1._2))) // counts line
                                  .map(line => (line._1, (line._2._1, line._2._2/2)))
                                  .sortBy(line => line._2._1)
    components.saveAsTextFile(args(1))
    System.out.println("Found components count:")
    System.out.println(components.count())
    System.out.println("Total iterations:")
    System.out.println(iterations)
  }
}
