package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level

object PageRankMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.PageRankMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)
    val k = 10

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//    val hadoopConf = new org.apache.hadoop.conf.Configuration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
		// ================
    val fakeEdges = sc.parallelize(for (i <- 1 to k) yield (i * k, 0))
    // val graph = sc.parallelize(for (i <- 1 to k - 1) yield
    //   for (j <- 1 to k - 1) 
    //     yield ((i - 1)*k + j, (i - 1)*k + j + 1)).flatMap(line => line)
    //              .union(fakeEdges)

    val graph = sc.parallelize(for (i <- 1 to k) yield (i - 1)*k + 1)
                 .flatMap(n => for (i <- 0 to k - 1) yield (n + i, n + i + 1))
                 .union(fakeEdges)

    var ranks = sc.parallelize(for (i <- 0 to k*k) yield (i, if (i==0) 0 else 1.asInstanceOf[Float]/(k*k)))

    var intermediate = graph.join(ranks)
                 .flatMap(line => Seq((line._1, 0.asInstanceOf[Float]), (line._2._1, line._2._2)))
                 .aggregateByKey(0.asInstanceOf[Float])(_ + _, _ + _)

    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
    counts.saveAsTextFile(args(1))
  }
}