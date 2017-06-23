drop table twitterData;
drop table HighestHashTag;
drop table HighestCount;

Create Table twitterData(TokenType string, month string, twitterCount INT, HashTagName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--LOAD DATA LOCAL INPATH './small_twitter.txt'
LOAD DATA LOCAL INPATH './1millionTweets.tsv'
	INTO TABLE twitterData;

Create table HighestHashTag AS
Select HashTagName, Sum(twitterCount) as twitCount
From twitterData
Group by HashTagName;
--Having MAX(twitterCount) = Select SUM(twitterCount);

Create table HighestCount AS
Select HashTagName, twitCount AS maxTwit
From HighestHashTag
Order By maxTwit Desc
limit 1;



 INSERT OVERWRITE LOCAL DIRECTORY './task2B/'
	        ROW FORMAT DELIMITED
	 	FIELDS TERMINATED BY '\t'
	        STORED AS TEXTFILE
	        Select * from HighestCount;



