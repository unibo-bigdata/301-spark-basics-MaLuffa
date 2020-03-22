import org.apache.spark.{SparkConf, SparkContext}

object InvertedIndex extends App {

  val conf = new SparkConf().setAppName("AverageWordLength Spark 1.6")
  val sc = new SparkContext(conf)

  val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra")

  val rddCapraWords = rddCapra.flatMap(x => x.split(" "))


  val rddInvertedIndex = rddCapraWords.zipWithIndex().groupByKey().collect
}
