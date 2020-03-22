import org.apache.spark.{SparkConf, SparkContext}

object WordLengthCount extends App {

  val conf = new SparkConf().setAppName("WordLengthCount Spark 1.6")
  val sc = new SparkContext(conf)

  val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra")

  val rddCapraWords = rddCapra.flatMap(x => x.split(" "))

  val rddCapraKV = rddCapra.map(word => (word.length,1))

  val rddCapraCount = rddCapraKV.reduceByKey((x,y) => x+y) //oppure reduceByKey(_+_)

  rddCapraCount.collect

}
