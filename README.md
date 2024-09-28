## About

This repository achieves **Temporal Data Management** in Exasol with the help of **query rewriting** using a preprocessor script. Temporal data refers to data that changes over time, and this feature enables tracking historical records while allowing for time-based queries. More indepth information about this proof of concept implementation can be found in the scientific paper *A Query-Rewriting-based Implementation for Temporal Data Management*.

## Installation 

Download the *Preprocessor.sql* file from this GitHub repository. This file contains a few lua scripts. Execute all of the scripts in your exasol instance in the order in which they appear. The schema in this example is called TEST. Note that you will need to rename it to match your database schema for it to work. Once all scripts have been executed, the preprocessor can be activated. To activate the preprocessor use the command below. <br> 
``` sql 
ALTER SESSION SET sql_preprocessor_script = historical_storage_preprocessor;
```

After activation each query will be rewritten by the preprocessor. If historical data storage is no longer required, the preprocessor can be deactivated with the following command:<br>
``` sql 
ALTER SESSION SET sql_preprocessor_script = null;
```

## Supported Commands

Due to its prototype nature this implementation does not support all possible SQL commands. Each query is confirmed with a status message. If this message is displayed, it means that the command has been executed successfully.  This message contains usefull information e.g. how many rows are affected by this change.

After activation, each newly created table has historical storage. This is accomplished using a history table and an SQL view with only current data. Each table has a corresponding history table with the prefix *HIST_* following the <table_name> and both tables can be queried like expected. Tables can be created with commands such as:<br>
``` sql 
CREATE TABLE <table_name> (<column_name> <DATATYPE>, ...);
CREATE TABLE <new_tbl> LIKE <orig_tbl>;
```

The following commands can be used to add new data records:<br>
``` sql 
INSERT INTO <table_name> (<column_name>, ...) VALUES (<value>, ...), ... ;
INSERT INTO <table_name> VALUES (<value>, ...), ... ;
INSERT INTO <table_name> (<column_name>, ...) SELECT (<column_name>, ...) FROM <other_table>;
```

To update an existing data set the subsequent command can be used:<br>
``` sql 
UPDATE <table_name> SET <column_name> = <value>, ... WHERE <condition>;
```

Data can be invalidated with a *DELETE* command. This syntax can also be used to empty a whole table without a *WHERE* clause. <br>
``` sql 
DELETE FROM <tbl_name> WHERE <condition>;
```

One of the main feature of this tool is to retrieve outdated data. For this, a *SELECT* query needs to be extended by a *AS OF SYSTEM TIME* clause. This should look like:<br>
``` sql 
SELECT <column_name> FROM <table_name> AS OF SYSTEM TIME 'YYYY-MM-DD HH:mm';
```
