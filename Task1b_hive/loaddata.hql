drop table mydata;
drop table income;

CREATE TABLE mydata(age INT, job string, martial string, education string, default string, balance INT, housing string, loan string, contact string, day INT, month string, duration int, campaign int, pdays int, previous int, poutcome string, termDeposit string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

LOAD DATA LOCAL INPATH './bank-full.csv'
	INTO TABLE mydata;

Create table income As
Select education, AVG(m.balance)
From mydata m
Group By education;


INSERT OVERWRITE LOCAL DIRECTORY './task1B/'
        ROW FORMAT DELIMITED
 	FIELDS TERMINATED BY '\t'
        STORED AS TEXTFILE
        Select * from income;