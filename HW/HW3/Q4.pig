A = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id: chararray, user_id: chararray, business_id: chararray, stars: double);
B = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id: chararray, full_address: chararray, categories: chararray);
B_FILTER = filter B by NOT($1 matches '.*CA.*'); 
AB = cogroup B_FILTER by $0, A by $2;
AB_FLATTEN = foreach AB generate flatten($1), flatten($2);
AB_GROUP = group AB_FLATTEN by ($0, $1, $2);
AB_NUMREVIEW = foreach AB_GROUP generate group, COUNT(AB_FLATTEN.$3) as NUM;   --review_id
--AB_DISTINCT = distinct AB_NUMREVIEW;
AB_ORDER = order AB_NUMREVIEW by NUM desc;
AB_TOP5 = limit AB_ORDER 5;
RESULT = foreach AB_TOP5 generate flatten(group);
store RESULT into '/rxy121130/HW3/Q4';


/*

pvlM--HZY1a8SqMXiwEz1A  32 Church StHarvard SquareCambridge, MA 02138   List(Cajun/Creole, Tex-Mex, Restaurants)
9YeSEzr8HZMCuQlAyr8FPw  4543 University Way NEUniversity DistrictSeattle, WA 98105      List(Thai, Restaurants)
yx4IYpg9MGbrVqNdZwXX_A  1 Kendall SqBldg 300Kendall Square/MITCambridge, MA 02139       List(Bars, Nightlife, Diners, Restaurants)
hzSyQBWeoX94WOpUtxuWVg  1246 Massachusetts AveHarvard SquareCambridge, MA 02138 List(Burgers, Restaurants)
CxQ1m2iY4wQpXC64tSfWgQ  1093 Hemphill Ave NWGeorgia TechAtlanta, GA 30318       List(Italian, Pizza, Restaurants)



*/
















/*

----------------------------------------------------------------------
Below are wrong ,just for making notes.

AB_NUMREVIEW = foreach AB generate $1, COUNT($2.review_id) as NUM;
--store AB_NUMREVIEW into '/rxy121130/HW3/q40';
AB_ORDER = order AB_NUMREVIEW by NUM desc;
AB_FILTER = filter AB_ORDER by $0 is not null;
AB_TOP10 = limit AB_FILTER 5;
store AB_TOP10 into '/rxy121130/HW3/q45';
RESULT = foreach AB_TOP10 generate flatten(COLUMNS);
store RESULT into '/rxy121130/HW3/Q4';




A = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id: chararray, user_id: chararray, business_id: chararray, stars: double);
B = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id: chararray, full_address: chararray, categories: chararray);
B_FILTER = filter B by NOT($1 matches '.*CA.*'); 
AB = join B_FILTER by $0, A by $2;
AB_GROUP = group AB by ($0, $1, $2);
AB_NUMREVIEW = foreach AB_GROUP generate group, COUNT(AB.$3) as NUM;   --review_id
AB_DISTINCT = distinct AB_NUMREVIEW;
AB_ORDER = order AB_DISTINCT by NUM desc;
AB_TOP10 = limit AB_ORDER 10;
RESULT = foreach AB_TOP10 generate flatten(group);
store RESULT into '/rxy121130/HW3/Q2';


A = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id: chararray, user_id: chararray, business_id: chararray, stars: double);
B = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id: chararray, full_address: chararray, categories: chararray);
AB = join B by $0, A by $2;
AB_FILTER = filter AB by NOT($1 matches '.*CA.*'); 
AB_GROUP = group AB_FILTER by ($0, $1, $2);
AB_NUMREVIEW = foreach AB_GROUP generate group, COUNT(AB_FILTER.$3) as NUM;   --review_id
AB_DISTINCT = distinct AB_NUMREVIEW;
AB_ORDER = order AB_DISTINCT by NUM desc;
AB_TOP10 = limit AB_ORDER 10;
RESULT = foreach AB_TOP10 generate flatten(group);
store RESULT into '/rxy121130/HW3/Q2';


*/