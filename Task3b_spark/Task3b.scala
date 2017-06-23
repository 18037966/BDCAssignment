//val document = sc.textFile("file:////home/cloudera/Desktop/Task3b_spark/docword_small.txt")
//val vocab = sc.textFile("file:////home/cloudera/Desktop/Task3b_spark/vocab_small.txt")

val document = sc.textFile("file:////home/cloudera/Desktop/Task3b_spark/docword.txt")
val vocab = sc.textFile("file:////home/cloudera/Desktop/Task3b_spark/vocab.txt")
val vocabKey = vocab.zipWithIndex.map(x => (x._2.toInt + 1, x._1))


val doc_split = document.map(_.split(" "))

val docKey = doc_split.map(x => (x(1).toInt, (x(0).toInt, x(2).toInt)))
val docGroup = docKey.groupByKey()

val result = vocabKey.join(docGroup)
val TempResult = result.map(x => (x._2))

def mySortWith(person1: (Int, Int), person2:(Int, Int))
: Boolean = {
   if(person1._2 > person2._2){
     	true
    }
   else
   {
	 false
   }
}

val FinalResult = TempResult.map(x => (x._1, x._2.toBuffer.sortWith( mySortWith(_, _) ))).sortBy(x => x._1, true)
FinalResult.saveAsTextFile("file:////home/cloudera/Desktop/Task3b_spark/task3b")
FinalResult.saveAsObjectFile("file:////home/cloudera/Desktop/Task3b_spark/InvertedIndex")








