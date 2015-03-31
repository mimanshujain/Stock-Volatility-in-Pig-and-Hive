drop table Stock_data;
create external table Stock_data 
(Date string, Open float, High float, Low float, Close float, Volume int, AdjClose float)
row format delimited fields terminated by ',' lines terminated by '\n'
location '/data'
tblproperties ("skip.header.line.count"="1");

drop table Filter_stock;
create table Filter_stock as
select regexp_extract(INPUT__FILE__NAME,'.*/(.*)(\\.csv)',1) as stockName, Date, substr(Date,0,7) as subDate, AdjClose from Stock_data;

drop table Stock_data;

drop table MaxMin_Data;
create table MaxMin_Data as
select stockName, subDate, max(Date) as maxDate, min(Date) as minDate from Filter_stock group by stockName, subDate;

drop table FindValue;
create table FindValue as
select M.stockName, F1.AdjClose as MaxPrice, F2.AdjClose as MinPrice, (F1.AdjClose- F2.AdjClose)/F2.AdjClose as xi
from MaxMin_Data as M
join Filter_stock as F1 on (F1.stockName = M.stockName and F1.subDate  = M.subDate) 
join Filter_stock as F2 on (F2.stockName = M.stockName and F2.subDate  = M.subDate) 
where F1.Date = M.maxDate and F2.Date = M.minDate;

drop table Filter_stock;
drop table MaxMin_Data;

drop table AscVolatility;
create table AscVolatility as
select stockName, stddev_samp(xi) as vol from FindValue group by stockName having vol != 0.0 sort by vol limit 10;

drop table DescVolatility;
create table DescVolatility as
select stockName, stddev_samp(xi) as vol from FindValue group by stockName sort by vol desc limit 10;

drop table FindValue;

select * from AscVolatility;
select * from DescVolatility;
