-- Avg Taxiin time to airport
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

flights = FOREACH data GENERATE origin, taxiin;  

grouped = GROUP flights by origin;

average = FOREACH grouped GENERATE FLATTEN(group), ROUND_TO(AVG(flights.taxiin),2);

STORE average INTO '/PIGOUTPUT/AvgTaxiinTime' USING PigStorage(',');
