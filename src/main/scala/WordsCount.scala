import org.apache.spark.{SparkConf, SparkContext}

object WordsCount extends App {
  val conf = new SparkConf().setAppName("WordsCount Spark 1.6")
  val sc = new SparkContext(conf)

  //leggo dal dataset capra e avrò: Array[String] = Array(sopra la panca la capra campa, sotto la panca la capra crepa)
  val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra")

  //splitto l'array per avere un array di singole parole, con la sola map avrei un array di array di singole parole
  val rddCapraWords = rddCapra.flatMap(x => x.split(" "))

  //ottengo un rdd chiave-valore associando ad ogni parola 1 per indicare che è presente
  val rddCapraKV = rddCapraWords.map(word => (word,1))

  //aggrego per chiave indicando che sommo i valori
  val rddCapraCount = rddCapraKV.reduceByKey((x,y) => x+y)

  //sputo in output l'rdd risultante
  rddCapraCount.collect

  //se voglio ordinare per chiave il risultato, in ordine alfabetico
  rddCapraCount.sortByKey().collect

  //se voglio ordinare in ordine decrescente per i valori devo rimapparli
  val rddCapraCountVK = rddCapraCount.map({case (word,count) => (count,word)})
  rddCapraCountVK.sortByKey(false).collect

}
