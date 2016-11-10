--pig
A = load '/yelpdatafall/review/review.csv' using PigStorage('^') as (review_id: chararray, user_id: chararray, business_id: chararray, stars: double);
B = load '/yelpdatafall/business/business.csv' using PigStorage('^') as (business_id: chararray, full_address: chararray, categories: chararray);
B_FILTER = filter B by NOT($1 matches '.*CA.*'); 
AB = join B_FILTER by $0, A by $2;
AB_GROUP = group AB by ($0, $1, $2);
AB_NUMREVIEW = foreach AB_GROUP generate group, COUNT(AB.$3) as NUM;   --review_id
--AB_DISTINCT = distinct AB_NUMREVIEW;
AB_ORDER = order AB_NUMREVIEW by NUM desc;
AB_TOP10 = limit AB_ORDER 10;
RESULT = foreach AB_TOP10 generate flatten(group);
store RESULT into '/rxy121130/HW3/Q2';


/*
Results:

pvlM--HZY1a8SqMXiwEz1A  32 Church StHarvard SquareCambridge, MA 02138   List(Cajun/Creole, Tex-Mex, Restaurants)
9YeSEzr8HZMCuQlAyr8FPw  4543 University Way NEUniversity DistrictSeattle, WA 98105      List(Thai, Restaurants)
yx4IYpg9MGbrVqNdZwXX_A  1 Kendall SqBldg 300Kendall Square/MITCambridge, MA 02139       List(Bars, Nightlife, Diners, Restaurants)
hzSyQBWeoX94WOpUtxuWVg  1246 Massachusetts AveHarvard SquareCambridge, MA 02138List(Burgers, Restaurants)
CxQ1m2iY4wQpXC64tSfWgQ  1093 Hemphill Ave NWGeorgia TechAtlanta, GA 30318      List(Italian, Pizza, Restaurants)
DfrvJL-6H5i1nsyL6H6VGg  233 Cardinal Medeiros AveKendall Square/MITCambridge, MA 02142  List(Southern, Restaurants)
gctbL0lDIYIHM6yDn99V-g  52 Brattle StSte DHarvard SquareCambridge, MA 02138    List(Food, Specialty Food, Coffee & Tea, Desserts, Chocolatiers and Shops)
58cDJU4cub1o0o0B4h9GrA  853 Main StCambridge, MA 02139  List(American (New), French, Restaurants)
_9ZZv5V-uM5BXx3P-HslIw  Harvard Square14 JFK StHarvard SquareCambridge, MA 02138List(Gastropubs, American (New), Restaurants)
C7FKNwSUlbUinT9cRSLXHA  795 Main StKendall Square/MITCambridge, MA 02139       List(Tapas/Small Plates, Bars, Nightlife, Lounges, Restaurants)


*/



















/*

dump AB_ORDER: 

(pvlM--HZY1a8SqMXiwEz1A,32 Church StHarvard SquareCambridge, MA 02138,List(Cajun/Creole, Tex-Mex, Restaurants)) 1848
(9YeSEzr8HZMCuQlAyr8FPw,4543 University Way NEUniversity DistrictSeattle, WA 98105,List(Thai, Restaurants))     1576
(yx4IYpg9MGbrVqNdZwXX_A,1 Kendall SqBldg 300Kendall Square/MITCambridge, MA 02139,List(Bars, Nightlife, Diners, Restaurants))  1446
(hzSyQBWeoX94WOpUtxuWVg,1246 Massachusetts AveHarvard SquareCambridge, MA 02138,List(Burgers, Restaurants))     1388
(CxQ1m2iY4wQpXC64tSfWgQ,1093 Hemphill Ave NWGeorgia TechAtlanta, GA 30318,List(Italian, Pizza, Restaurants))    1374
(DfrvJL-6H5i1nsyL6H6VGg,233 Cardinal Medeiros AveKendall Square/MITCambridge, MA 02142,List(Southern, Restaurants))     1272
(gctbL0lDIYIHM6yDn99V-g,52 Brattle StSte DHarvard SquareCambridge, MA 02138,List(Food, Specialty Food, Coffee & Tea, Desserts, Chocolatiers and Shops)) 1184
(58cDJU4cub1o0o0B4h9GrA,853 Main StCambridge, MA 02139,List(American (New), French, Restaurants))       1128
(_9ZZv5V-uM5BXx3P-HslIw,Harvard Square14 JFK StHarvard SquareCambridge, MA 02138,List(Gastropubs, American (New), Restaurants))1128
(C7FKNwSUlbUinT9cRSLXHA,795 Main StKendall Square/MITCambridge, MA 02139,List(Tapas/Small Plates, Bars, Nightlife, Lounges, Restaurants))       1068

Note:

can't exchange the positions of limit and last foreach or it will be crashed.

*/

