# sqlsafecsv
Command line tool that allows you to parse a csv ready for ingestion into a sql database (preventing dt conflicts and overflows)

PLEASE NOTE THIS IS STILL A WIP AND IS ONLY DESIGNED TO WORK WITH DATA TYPES ASSOCIATE TO AWS REDSHIFT

## Installation:

### run the foolowing commands:

#### For Ubuntu:  
```
wget https://github.com/Stew2134/sqlsafecsv/releases/download/v0.2/sqlsafecsv.tar
tar -xf sqlsafecsv.tar
chmod -u+x sqlsafecsv
mv sqlsafecsv /usr/bin/
```

(use sudo if permissions are denied on ubuntu)    

#### For Alpine:  
```
wget https://github.com/Stew2134/sqlsafecsv/releases/download/v0.2/sqlsafecsv_alpine
tar -xf sqlsafecsv_alpine.tar
chmod -u+x sqlsafecsv_alpine
mv sqlsafecsv_alpine /usr/bin/sqlsafe
```

## Usage:

analyze your raw csv making note of all the columns you want to print into the output file
then create a mapping csv in the following format:

field_name,data_type  
column_1,varchar(10)  
column_2,integer  
column_3,float  
column_4,timestamp  
column_5,timestamptz  
column_6,date  
column_7,boolean  

(See list of datatypes and formats accepted at the bottom of the readme)  

once this is created you can use the program in terminal via the following method:  

```
sqlsafecsv <mapping>.csv <input>.csv
```

this will output the parsed csv via stdout so you can read the corrections made before saving the output file  

to save to an output file use std linux operator for pushing stdout to file input:  

```
sqlsafecsv <mapping>.csv <input>.csv > <output>.csv
```

### Please note the only following redshift datatypes are currently supported:

- VARCHAR:
    - Important to note varchar max is not supported and then actual size of the value has to be specified 

- INTEGER:
    - This will first cast any number into a 64 bit float and then round to the nearest whole number and then proceed to cast to an integer 

- FLOAT:
    - This will parse a 32 bit float 

- TIMESTAMP : 
    - formats supported: 
        - YYYY-MM-DD HH:MM:SS 

    - if the value in the csv does not match one of the formats above a blank string will be passed in to ensure safety 

- TIMESTAMPTZ: 
    - formats supported: 
        - YYYY-MM-DD HH:MM:SS.f :z (e.g. 2025-07-28 01:53:15.123456 +00:00 ) 
        - unix timestamp 

    - this will parse all timestamp with time zone formats listed above to the redshift default 

- DATE: 
    - formats supported: 
        - YYYY-MM-DD 
        - DD/MM/YYYY  

    - if the value in the csv does not match on one of the above formats a blank string will be passed in to ensure safety 

- BOOLEAN 
    - formats_supported: 
        - true,false 
        - yes,no 
        - 1,0 

    - (FORMATS ARE ALL CASE INSENSITIVE SO CAPITALIZED CHARARACTERS ARE STILL CONSIDERED) 

    if the value in the csv does not match on one of the above formats a blank string will be passed in to ensure safety 
