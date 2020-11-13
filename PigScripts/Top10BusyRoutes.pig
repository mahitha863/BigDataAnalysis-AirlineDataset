-- TOP10 BUSY ROUTES
data = LOAD '/flightData' USING PigStorage(',') AS 
	(year: int, month: int, day: int, dayweek: int, 
	deptime: int, crsdeptime: int, arrtime: int, crsarrtime: int, 
	carrier: chararray, flightnum: int, tailnum: chararray, 
	actelaptime: int, crselaptime: int, airtime: int, 
	arrdelay: int, depdelay: int, 
	origin: chararray, dest: chararray, dist: int, 
	taxiin: int, taxiout: int, 
	cancelled: chararray, cancelcode: chararray, diverted: int, 
	carrdelay: int, weadelay: int, nasdelay: int, secdelay: int, lateacdelay: int);



trip = FOREACH data GENERATE origin, dest;

grouped = GROUP trip by (origin, dest);

route = FOREACH grouped GENERATE group, COUNT(trip) as nooftrips;

sorted = ORDER route BY nooftrips DESC;

top10 = LIMIT sorted 10;

STORE top10 INTO '/PIGOUTPUT/Top10BusyRoutes' USING PigStorage(',');
