-- Average departure delay time for each day in week analysed for span of 22 years
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

flights = FOREACH data GENERATE year, dayweek, depdelay;  

grouped = GROUP flights by (year,dayweek);

avg = FOREACH grouped GENERATE FLATTEN(group), ROUND_TO(AVG(flights.depdelay),2);

STORE avg INTO '/PIGOUTPUT/AvgDelayPerDay' USING PigStorage(',');
