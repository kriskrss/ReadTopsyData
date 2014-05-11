ReadTopsyData
=============
###Run DataImporter with following options

* -q : The query string
* -st : start time
* -et : end time
* -i : interval

###Things to keep in mind
* The query string must be specified
* The format of the data and time is yyyy-MM-dd;HH:MM:SS.
* If start time is not specified, it is assumed to be 2006-01-01;00-00-00
* If end time is not specified, it is assumed to be current time
* The interval that the user specified is tried, and if the volume of records is found to be high, the iterval is halfed and the program tries to download the data again

###Example:
java DataImporter -q "rahul gandhi" -st 2014-05-05;01:00:00 -i 86400
