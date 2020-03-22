import org.apache.spark.{SparkConf, SparkContext}

object AverageWordLength extends App {
  val conf = new SparkConf().setAppName("AverageWordLength Spark 1.6")
  val sc = new SparkContext(conf)

  val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra")

  val rddCapraWords = rddCapra.flatMap(x => x.split(" "))

  val rddCapraKV = rddCapraWords.map(word => (word.substring(0,1), word.length))

  val rddCapraLengthPerLetter = rddCapraKV.aggregateByKey((0.0, 0.0))((a, v) => (a._1 + v, a._2 + 1), (a1, a2) => (a1._1 + a2._1, a1._2 + a2._2))

  val rddCapraAvgLength = rddCapraLengthPerLetter.mapValues({case (sum, count) => sum/count}) //oppure map({ case (k, v) => (k, v._1 / v._2) }) //oppure mapValues(v => v._1/v._2)

  rddCapraAvgLength.collect
}
