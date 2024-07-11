package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.storage.StorageLevel
import java.io.PrintWriter

object PageRankMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.PageRankMain <input dir> <output dir>")
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
    val fakeEdges = sc.parallelize(for (i <- 1 to k) yield (i * k, 0))
    // val graph = sc.parallelize(for (i <- 1 to k - 1) yield
    //   for (j <- 1 to k - 1) 
    //     yield ((i - 1)*k + j, (i - 1)*k + j + 1)).flatMap(line => line)
    //              .union(fakeEdges)

    val graph = sc.parallelize(for (i <- 1 to k) yield (i - 1)*k + 1)
                 .flatMap(n => for (i <- 0 to k - 1) yield (n + i, n + i + 1))
                 .union(fakeEdges)

    var ranks = sc.parallelize(for (i <- 0 to k*k) yield (i, if (i==0) 0 else 1.asInstanceOf[Float]/(k*k)))
    var outputString = ""

    for (iter <- 1 to ITERATIONS) {
      val intermediate = graph.join(ranks)
                  // here we dump ALL of a node's value into ALL of its neighbors, which works due to the special graph structure
                  .flatMap(line => Seq((line._1, 0.asInstanceOf[Float]), (line._2._1, line._2._2)))
                  .aggregateByKey(0.asInstanceOf[Float])(_ + _, _ + _)
      val zeroRank = intermediate.collectAsMap().get(0).get
      ranks = intermediate.map(line => if (line._1 != 0) (line._1, line._2 + zeroRank/(k*k)) else {
        System.out.println(f"Iteration $iter%s")
        (0, 0.asInstanceOf[Float])
      })
      outputString = outputString.concat("\n\n")
      outputString = outputString.concat(f"Iteration $iter%s")
      outputString = outputString.concat("\n\n")
      outputString = outputString.concat(ranks.toDebugString)
      //System.out.println(ranks.toDebugString)
      //ranks.persist(StorageLevel.MEMORY_AND_DISK)
    }
    ranks.sortByKey().saveAsTextFile(args(1))
    Some(new PrintWriter("./output/log.txt")).foreach{p => p.write(outputString); p.close}
    //val sum = ranks.map(line => (1, line._2)).reduceByKey(_ + _).map(_._2).foreach(println)
  }
}