package org.spark.simpleapp

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object myapp {
  def main(args: Array[String]) {
    val conf = new SparkConf()
                .setAppName("My Simple Application")
                .setMaster("local")
    val sc = new SparkContext(conf)
    val logFile = "/Applications/Spark/spark-2.0.1-bin-hadoop2.7/README.md" // Should be some file on your system
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
