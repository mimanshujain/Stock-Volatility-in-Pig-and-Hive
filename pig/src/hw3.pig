A = LOAD 'hdfs:///pigdata' USING PigStorage(',','-tagFile') as (fileName, Date,Open, High, Low, Close, Vol, AdjClose);	
B = FILTER A by Open != 'Open';

C = FOREACH B generate (chararray)$0,(chararray)$1,(double)$7;

D = GROUP C BY (fileName, SUBSTRING(Date, 0, 7));

E = FOREACH D GENERATE $0, MAX(C.Date) AS max_Date;
F = FOREACH D GENERATE $0, MIN(C.Date) AS min_Date;

G = JOIN E BY (max_Date,$0.$0), C By (Date,fileName);
H = JOIN F BY (min_Date,$0.$0), C By (Date,fileName);

I = JOIN G BY $0, H BY $0;
J = FOREACH I GENERATE  $0, ($9 - $4)/$9 as xi;

K = GROUP J BY $0.$0;
L = FILTER K BY COUNT(J.$1) > 1;
M = FOREACH L GENERATE $0 as stockName, SUM(J.$1)/COUNT(J.$1) as mean;
N = FOREACH L GENERATE $0 as stockName, COUNT(J.$1) as totalMonths;

STEP1 = JOIN J BY $0.$0, M BY $0; 
STEP2 = FOREACH STEP1  GENERATE $0, (xi - mean)*(xi - mean) as diff;
STEP3 = GROUP STEP2 BY $0.$0;
STEP4 = FOREACH STEP3 GENERATE $0 as stockName, SUM(STEP2.diff) as sumValues;
STEP5 = JOIN STEP4 BY $0, N BY $0;
STEP7 = FOREACH STEP5 GENERATE $0, SQRT(sumValues/(totalMonths - 1)) as VOLATILITY;
STEP8 = FILTER STEP7 BY VOLATILITY != 0;
STEP9 = ORDER STEP8 BY VOLATILITY;
minVolatility = LIMIT STEP9 10;
STEP10 = ORDER STEP8 BY VOLATILITY DESC;
maxVolatility = LIMIT STEP10 10;

MAX_MIN = UNION maxVolatility,minVolatility;
STORE MAX_MIN INTO 'hdfs:///pigdata/hw3_out';


