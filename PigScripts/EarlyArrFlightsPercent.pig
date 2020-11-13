-- Percentage of flights that arrived earlier than expected per year
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

flights = FOREACH data GENERATE year, arrdelay;

grouped = GROUP flights BY year;

calc_perc = FOREACH grouped {
                earlyarr = FILTER flights BY (arrdelay<0); -- negative means arrived early
                GENERATE group, COUNT(earlyarr) AS early, COUNT(flights) AS total, 
                                ROUND_TO((float) COUNT(earlyarr)/COUNT(flights) * 100, 2) AS percentage;
            }


STORE calc_perc INTO '/PIGOUTPUT/EarlyArrFlightsPercent' USING PigStorage(',');
