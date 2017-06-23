drop table mydata;
drop table Martialstatus;
drop table MartialStatus2;
drop table MartialPercentage;

CREATE TABLE mydata(age INT, job string, martial string, education string, default string, balance INT, housing string, loan string, contact string, day INT, month string, duration int, campaign int, pdays int, previous int, poutcome string, termDeposit string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;';

LOAD DATA LOCAL INPATH './bank-full.csv'
	INTO TABLE mydata;


CREATE TABLE Martialstatus AS
Select martial, count(*) AS Percount    
From mydata 
Where loan = "\"yes\""
Group by martial;

CREATE TABLE MartialStatus2 AS
Select martial, count(*) As total
From mydata 
Group By martial;



CREATE TABLE MartialPercentage AS
Select m.martial, ((m.Percount * 100) / m1.total) AS result
From Martialstatus m, MartialStatus2 m1
where m.martial = m1.martial;

 INSERT OVERWRITE LOCAL DIRECTORY './task1C/'
	        ROW FORMAT DELIMITED
	 	FIELDS TERMINATED BY '\t'
	        STORED AS TEXTFILE
	        Select * from MartialPercentage;