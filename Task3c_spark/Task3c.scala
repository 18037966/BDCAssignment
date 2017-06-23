val loadResult = sc.objectFile[(String,String)]("file:////home/cloudera/Desktop/Task3b_spark/InvertedIndex")

val fresult = loadResult.filter(x=>x._1=="aaa")