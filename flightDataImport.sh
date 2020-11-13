#!/bin/bash
FILES=/MAHI_NEU/EBD/Project/data/*.csv
for f in $FILES
do
 echo "Processing $f file..."
 # set MONGODB_HOME environment variable to point to the MongoDB installation folder
 MONGODB_HOME = /MAHI_NEU/EBD/packages/mongodb
 ls -l $f
 /MAHI_NEU/EBD/packages/mongodb/bin/mongoimport --type csv --db flightdb --collection flight --headerline --file $f
 # $MONGODB_HOME/bin/mongoimport --type csv --db flightdb --collection flight --headerline --file $f
done 
