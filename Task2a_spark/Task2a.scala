val lines = sc.textFile("file:////home/cloudera/Desktop/Task2a_spark/1millionTweets.tsv")
//val lines = sc.textFile("file:////home/cloudera/Desktop/Task2a_spark/small_twitter.txt")

val split_lines = lines.map(_.split("\t"))
val result = split_lines.map(x => (x(1), x(2).toInt, x(3)))

def maxBy(person1: (String, Int, String), person2:(String, Int, String))
: (String, Int, String) = {
	if(person1._2 > person2._2){
		person1
	}
	else
	{
		person2
	}
}


val fresult = result.reduce((x, y) => maxBy(x, y))



