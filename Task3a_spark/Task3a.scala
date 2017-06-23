//val document = sc.textFile("file:////home/cloudera/Desktop/Task3a_spark/docword_small.txt")
//val vocab = sc.textFile("file:////home/cloudera/Desktop/Task3a_spark/vocab_small.txt")

val document = sc.textFile("file:////home/cloudera/Desktop/Task3a_spark/docword.txt")
val vocab = sc.textFile("file:////home/cloudera/Desktop/Task3a_spark/vocab.txt")
val doc_split = document.map(_.split(" "))
//doc_split.collect

val docKey = doc_split.map(x => (x(1).toInt, x(2).toInt))
//docKey.collect

val docGroup = docKey.groupByKey()

//val vocab_sp = vocab.map(_.split(" "))    
val vocabKey = vocab.zipWithIndex.map(x => (x._2.toInt + 1, x._1))

val result = vocabKey.join(docGroup)

val TempResult = result.map(x => (x._2))

val FinalResult = TempResult.map(x => (x._1, x._2.reduce(_ + _))).sortBy(x => x._1, true)

FinalResult.saveAsTextFile("file:////home/cloudera/Desktop/Task3a_spark/task3a")
//val FinalResult = TempResult.groupByKey()


