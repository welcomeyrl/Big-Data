-- pig
A = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id: chararray, user_id: chararray, business_id: chararray, stars: double);
B = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id: chararray, full_address: chararray, categories: chararray);
B_FILTER = filter B by ($1 matches '.*CA.*');
AB = join B_FILTER by $0, A by $2;
AB_GROUP = group AB by ($0, $1, $2);
AB_AVG = foreach AB_GROUP generate group, AVG(AB.$6) as AVERAGE;
--AB_DISTINCT = distinct AB_AVG;
AB_ORDER = order AB_AVG by AVERAGE desc;
AB_TOP10 = limit AB_ORDER 10;
RESULT = foreach AB_TOP10 generate flatten(group);
store RESULT into '/rxy121130/HW3/Q1';

/*
ctrl c 
hdfs dfs -cat /rxy121130/HW3/Q1/*

yj_BRB9rFnTGNbjUbokw4Q  1318 Monte Vista AveSte 1Upland, CA 91786       List(Home Services, Heating & Air Conditioning/HVAC)
ys0oidKmKNo6j3Dh_k8R_w  960 E Green StSte 292PasadenaPasadena, CA 91106 List(Doctors, Health and Medical, Family Practice)
z-TSgFfU0GY2wLVshrdAag  1330 N Monte Vista AveSte 2Upland, CA 91786     List(Active Life, Martial Arts, Fitness & Instruction)
-iPKb_EYHk8Ce9qwgXIvBg  South Los AngelesSouthern California, CA 90089  List(Movers, Home Services)
-cmJ9eR4L8JB0aO_CbuGng  1701 Monterey StSan Luis Obispo, CA 93401       List(Auto Repair, Automotive, Body Shops)
zfJlkPH6VmuFfZBAkRr2kw  1114 Garden StSan Luis Obispo, CA 93401 List(Bridal, Shopping, Jewelry)
zrmO-d-Mw3kv9dWa7Trr9Q  1101 Welch RdSte A1Palo Alto, CA 94304  List(Doctors, Health and Medical, Pediatricians)
-6fMAgRynU3iL_ZbQV3eLw  1915 W Arrow RteUpland, CA 91786        List(Home Services, Heating & Air Conditioning/HVAC)
zvo21oKr656PQNVblYxYlg  150 E Tenth StClaremont, CA 91711       List(Colleges & Universities, Education)
PmUsCj_HFGLXraJbz92uVQ  2732 Channing WayUC Campus AreaBerkeley, CA 94704       List()


*/




