-- Min Max departure delay time for each flight in month analysed for span of 22 years
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

flights = FOREACH data GENERATE dayweek, flightnum, depdelay;

delayedflights = FILTER flights BY (depdelay>0);

grouped = GROUP delayedflights by (dayweek,flightnum);

minmax = FOREACH grouped GENERATE FLATTEN(group), MIN(delayedflights.depdelay), MAX(delayedflights.depdelay);

STORE minmax INTO '/PIGOUTPUT/MinMaxDelayPerDayWeek' USING PigStorage(',');
